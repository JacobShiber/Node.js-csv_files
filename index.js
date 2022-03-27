//CVS parser - task 1

require('dotenv').config();
const fetch = require('node-fetch');
const fs = require('fs');
const cvsParses = require('csv-parser');
const express = require('express');
const app = express();
const port = process.env.PORT;
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
const NodeCache = require('node-cache');

const EventEmitter = require("events").EventEmitter;

const myEventEmitter = new EventEmitter(); 



//task 1 functions -- 
const fetchData = async () => { //this function is to fetch the data from the external api and parse it from json
    const response = await fetch('https://www.balldontlie.io/api/v1/players'); //here im fetching the data from the external api
    const responseData = response.json(); // here im converting the data from json to js
    return responseData; //returning the data
}


const checkPlayersInfo = (dataFromAPI) => { //this function check if the players I have in the csv file are matching by id to the external data and if so it creates new array that contain the full data of each player
    const outSourceData = dataFromAPI.data; //gets the data from the out source api
    const playersFullInfo = []; // here I store all the matching data from the api 

    const dataFromCsv = []; // the data from the csv file
    fs.createReadStream('./players.csv')
        .pipe(cvsParses({}))
        .on('data', (data) => dataFromCsv.push(data))
        .on('end', () => {

            for (let i = 0; i < dataFromCsv.length; i++) {
                parseInt(dataFromCsv[i].id) === outSourceData[i].id ?
                    playersFullInfo.push(outSourceData[i]) : playersFullInfo.push(dataFromCsv[i]); // here i push to the new array if there is a match by id. if not i keep the player from my own csv file.
            }
            writeToCsvFile(playersFullInfo) //here i call to my function to write the new data into the csv file
        })

}

myEventEmitter.on('change', () => { // this is for task 4, in here I create an event that happens everytime a change has happend in the players details
    console.log('There has been a change in the players details');
})


const writeToCsvFile = (csvData) => { // in this function i am writing to csv file im getting my updated data as argument and writing it to the csv file
    const csvWriter = createCsvWriter({
        path: 'players1.csv',
        header: [
            { id: 'id', title: 'ID' },
            { id: 'firstname', title: 'FIRST-NAME' },
            { id: 'lastname', title: 'LAST-NAME' },
            { id: 'position', title: 'POSITION' },
            { id: 'height', title: 'HEIGHT' }
        ]
    });

    const records = [
        
    ];
    csvData.map(player => {
        records.push({id: player.id, firstname: player.first_name, lastname: player.last_name, position: player.position, height: player.height_inches})
    })


    csvWriter.writeRecords(records)    
        .then(() => {
            console.log('...Done');
            myEventEmitter.emit('change'); // here i launch my event since a change has happend to my file
        });
}

fetchData().then(data => { // here i call to my fetch function and then calling to my checkPlayersInfo function for comparing the data
    checkPlayersInfo(data);
});

//task 2 - persistent caching 

const myCache = new NodeCache({stdTTL: 10})

//task 3 - update the cache every 15 minutes 

const getDataForCache = () => { //here im getting the data from the csv file to set it in the cache every 15 minutes
    const dataForCache = [];
    fs.createReadStream('./players1.csv')
            .pipe(cvsParses({}))
            .on('data', (data) => dataForCache.push(data))
            .on('end', () => {
                myCache.set('players', dataForCache);
            })
}




setInterval(getDataForCache, 900000); // this is the setinterval function to put the csv data in the cache every 15 minutes


app.use('/player', (request, respond) => {

    if(myCache.has('players')){ // if my cache has data the it will send it from the cache
        console.log('from cache');
        return respond.status(200).send(myCache.get('players'));
        
    }
    else{ // else ill send the data from the file and set my cache
        console.log('from file')
        const dataFromCsv = [];
        fs.createReadStream('./players1.csv')
            .pipe(cvsParses({}))
            .on('data', (data) => dataFromCsv.push(data))
            .on('end', () => {
                myCache.set('players', dataFromCsv);
                respond.status(200).send(dataFromCsv);
            })
    }
})

app.get('/', (req, res) => {
    res.status(200).send('Server is up');
})

app.listen(port);