const { MongoClient } = require('mongodb')

const uri = 'mongodb://localhost:27017'
const dbName = 'plp_bookstore'
const collectionName = 'books'

async function insertBooks() {
    const client = new MongoClient(uri)

    try {
        await client.connect()
        console.log('Connected to Database...')

        const db = client.db(dbName)
        const books = db.collection(collectionName)

        //-------------------Task #2------------------//
        // - Find all books in a specific genre
            const query1 = await books.find({ genre: 'Fiction' }).toArray()
            console.log("All books: ", query1)

        // - Find books published after a certain year
            const quer2 = await books.aggregate([
                { $match: { published_year: { $gt: 1950 } } }
            ]).toArray()
            console.log('Books published after 1950: ', quer2)

        // - Find books by a specific author
                // const query3 = await books.find({ author: 'Paulo Coelho'}).toArray()
                const query3 = await books.aggregate([
                    { $match: { author: 'Paulo Coelho' } }
                ]).toArray()
                console.log('Books by Paulo Coehlo: ', query3)

        // - Update the price of a specific book
            const booksList = await books.find().toArray()
            console.log('All Books: ', booksList)

            // // I will update the book using the title field - Moby Dick  
            await books.updateOne({ _id: '68419c9e8a6c15a90f4ea1f5'} , [{ $set: {"price": 15 }}])

            // Get the book...price is updated from 12.5 to 15
            const updated = await books.find().toArray()
            console.log('Updated book price: ', updated)
        // - Delete a book by its title
            await books.deleteOne({title: 'Animal Farm'})
            console.log('Deleted book: The Animal Farm!')


//----------------### Task 3: Advanced Queries----------------------//
// - Write a query to find books that are both in stock and published after 2010
        const q = await books.find({
            in_stock: true,
            published_year: { $gt: 2010}
        }).toArray()
        console.log('Published after 2010 and in stock: ', q)

// - Use projection to return only the title, author, and price fields in your queries
        const projection = await books.find().project({ title: 1, author: 1, price: 1 }).toArray()
        console.log("Show only selected field: ", projection)

// - Implement sorting to display books by price (both ascending and descending)
// assending is 1 while descending is -1
// some documents have simialar values for the field , eg there are 2 10.99 price field...we will also sort the using the _id field to h=get consisitent results
        const ascending = await books.aggregate([
            { $sort: { price: 1, _id: 1}}
        ]).toArray()
        console.log('Sorted price in ascending order: ', ascending)

        const descending = await books.aggregate([
            { $sort: { price: -1, _id: -1}}
        ]).toArray()
        console.log("Sorted using price in descending oerder: ", descending)


// - Use the `limit` and `skip` methods to implement pagination (5 books per page)
        const limit = await books.aggregate([
            { $limit: 5 }
        ]).toArray()
        console.log('Results limited first to 5 results: ', limit)

        const skip = await books.aggregate([
            { $skip: 5 }
        ]).toArray()
        console.log('Skipped the first 5 documents: ', skip)

//-----------------### Task 4: Aggregation Pipeline------------------//
// - Create an aggregation pipeline to calculate the average price of books by genre
        const avgpriceGenre = await books.aggregate([
            {
                $group: { _id: '$genre', avgPrice: {$avg: '$price' } }
            }
        ]).toArray()
        console.log('Average price of books by genre: ', avgpriceGenre)

// - Create an aggregation pipeline to find the author with the most books in the collection
        const maxBooks = await books.aggregate([
            { $group: { _id: '$author', totalBooks: { $sum: 1 } } },
            { $sort: { totalBooks: -1, _id: -1 } },
            { $limit: 1 }
        ]).toArray()
        console.log('Author with most books: ', maxBooks)

// - Implement a pipeline that groups books by publication decade and counts them
//the $mod operator claculates the years with the rem of 10 
        const grpPublication = await books.aggregate([
            {
                $group: {
                _id: { $subtract: ["$published_year", { $mod: ["$published_year", 10] }] },
                count: { $sum: 1 }
                }
            },
            { $sort: { _id: 1 } }
        ]).toArray()
        console.log('Sorted by publication decade: ', grpPublication)


//-----------------### Task 5: Indexing--------------------//
// - Create an index on the `title` field for faster searches
        await books.createIndex({ title: 1 })

// - Create a compound index on `author` and `published_year`
        await books.createIndex({ author: 1, published_year: -1 })

// - Use the `explain()` method to demonstrate the performance improvement with your indexes
// Stable API
// The Stable API V1 supports the following verbosity modes for the explain command:

// allPlansExecution

// executionStats

// queryPlanner

//"indexName": "title_1" -- from the JSon we see that the title fiedl has an index named title_1
        const explain = await books.find({ title: '1984'}).explain('executionStats')
        console.log(JSON.stringify(explain, null, 2))

    } catch (error) {
        console.log(error)
    } finally {
        client.close()
        console.log('Database connection closed...')
    }
}

insertBooks()

