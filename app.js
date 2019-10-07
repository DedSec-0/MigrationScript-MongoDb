const async = require('async')
const mongodb = require('mongodb')
const customer_address_data = require('./data/customer-address-data.json')
const customer_data = require('./data/customer-data.json')

const url = 'mongodb://localhost:27017'
const noOfQuery = parseInt(process.argv[2]) || customer_data.length
var tasks = []


mongodb.MongoClient.connect(url, {useNewUrlParser: true, useUnifiedTopology: true})
.then((client) => {

    customer_data.forEach((data, index) => {
        var startIndex = 0;
        var endingIndex = 0;
        customer_data[index] = Object.assign(data, customer_address_data[index])
    
        if((index % noOfQuery) == 0){
            startIndex = index;
            endingIndex = startIndex + noOfQuery
            tasks.push((done) => {
                console.log('Inserting Data => ' + startIndex + ' to ' + endingIndex)
                client.db('exchange').collection('customers').insertMany(customer_data.slice(startIndex, endingIndex), (error, result) => {
                    if(error)
                        done(error, null)
                    done(null, result)
                    console.log('Data inserted from ' + startIndex + ' to ' + endingIndex)
                })
            })
        }
    });

    console.log(`Launching ${tasks.length} parallel task(s)`);
    async.parallel(tasks, (error, result) => {
        if (error) console.error("Async: " + error)
        else console.log(result.length + " Feilds inserted")

        client.close()
    })
})
.catch((error) => {
    console.error("Db: " + error)
    mongodb.MongoClient.close()
})