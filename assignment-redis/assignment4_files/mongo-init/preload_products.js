db = db.getSiblingDB('product_catalog');

// 1 GB Data set

const maxItems = 10000;
const dataSize = 1024 * 100; // 100 KB

// Generate a string of specified size
function generateData(size) {
    return 'x'.repeat(size);
}

// Generate products array
let products = [];
for (let i = 1; i <= maxItems; i++) {
    products.push({
        "_id": i.toString(),
        "name": `Product-${i}`,
        "price": -1,
        "data": generateData(dataSize)
    });
}

// Insert the generated products
db.products.insertMany(products);
