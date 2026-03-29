# product_service.py

from fastapi import FastAPI, HTTPException
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.semconv.resource import ResourceAttributes

import uvicorn
from typing import Dict, List
import logging

from prometheus_client import Counter, Histogram, Gauge, generate_latest

app = FastAPI()

#############################
# TRACING CONFIGURATION
#############################

FastAPIInstrumentor.instrument_app(
    app,
    excluded_urls="/health,/ready,/metrics,/toggle-liveness,/toggle-readiness"
)

resource = Resource(attributes={
    ResourceAttributes.SERVICE_NAME: "products"
})

tracer_provider = TracerProvider(resource=resource)
trace.set_tracer_provider(tracer_provider)
otlp_exporter = OTLPSpanExporter(
    endpoint="http://tempo.observability.svc:4317")
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(otlp_exporter))

##############################
# Logging Configuration
##############################


class TraceIdFilter(logging.Filter):
    def filter(self, record):
        current_span = trace.get_current_span()
        if current_span:
            context = current_span.get_span_context()
            if context:
                record.trace_id = format(context.trace_id, '016x')
            else:
                record.trace_id = 'NO_TRACE'
        else:
            record.trace_id = 'NO_TRACE'
        return True


log_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "filters": {
        "trace_id_filter": {
            "()": TraceIdFilter
        }
    },
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(name)s - [trace_id=%(trace_id)s] - %(levelname)s - %(message)s",
        }
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stderr",
            "filters": ["trace_id_filter"]
        }
    },
    "loggers": {
        "products": {"handlers": ["default"], "level": "INFO"},
        "uvicorn": {"handlers": ["default"], "level": "INFO"},
        "uvicorn.error": {"handlers": ["default"], "level": "WARNING"},
        "uvicorn.access": {"handlers": ["default"], "level": "WARNING"}
    }
}

# Initialize logging with the config
logging.config.dictConfig(log_config)
logger = logging.getLogger("products")

####################################
# Prometheus Metrics - Scenario 4
####################################

# TODO: Add relevant Prometheus metrics as part of Scenario 4


#############################

# Global flags to simulate application readiness
is_live = True
is_ready = True

######################################
# Probes Scenario API Endpoints
######################################


@app.get("/toggle-liveness")
async def toggle_ready():
    global is_live
    is_live = not is_live
    logger.info(f"Readiness status toggled to: {is_live}")
    return {"is_live": is_live}


@app.get("/toggle-ready")
async def toggle_ready():
    global is_ready
    is_ready = not is_ready
    logger.info(f"Readiness status toggled to: {is_ready}")
    return {"ready": is_ready}


@app.get("/health")
async def health():
    if not is_live:
        raise HTTPException(
            status_code=503, detail="Service Unavailable - Unhealthy")
    return {"status": "healthy"}


@app.get("/ready")
async def ready():
    if not is_ready:
        raise HTTPException(
            status_code=503, detail="Service Unavailable - Not Ready")
    return {"status": "ready"}


###################################################
# PRODUCTS APPLICATION LOGIC - SCENARIO 1 - 4
###################################################

# In-memory product database
products: Dict[str, Dict] = {
    "PROD001": {"id": "PROD001", "name": "Laptop", "price": 999.99, "stock": 10},
    "PROD002": {"id": "PROD002", "name": "Mouse", "price": 24.99, "stock": 50},
}


@app.get("/products")
async def get_products() -> List[Dict]:
    return list(products.values())


###################################################
# ENDPOINTS THAT ARE CALLED BY ORDERS IN SCENARIO 5
###################################################

@app.get("/products/{product_id}")
async def get_product(product_id: str) -> Dict:
    if product_id not in products:
        raise HTTPException(status_code=404, detail="Product not found")
    return products[product_id]


@app.post("/products/{product_id}/reserve")
async def reserve_product(product_id: str, quantity: int) -> Dict:
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("reserve_product") as span:
        span.set_attribute("product_id", product_id)
        span.set_attribute("requested_quantity", quantity)

        if product_id not in products:
            span.set_attribute("error_message", "product_not_found")
            span.set_status(trace.Status(trace.StatusCode.ERROR))
            raise HTTPException(status_code=404, detail="Product not found")

        if products[product_id]["stock"] < quantity:
            logger.info(
                f"Insufficient stock for product {product_id}. Available: {products[product_id]['stock']}, Requested: {quantity}")
            span.set_status(trace.Status(trace.StatusCode.ERROR))
            span.set_attribute("error_message", "insufficient_stock")
            raise HTTPException(status_code=400, detail="Insufficient stock")

        products[product_id]["stock"] -= quantity
        span.set_attribute("remaining_stock", products[product_id]["stock"])

        return {
            "product_id": product_id,
            "reserved_quantity": quantity,
            "remaining_stock": products[product_id]["stock"]
        }

#######
# MAIN
#######


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_config=log_config)
