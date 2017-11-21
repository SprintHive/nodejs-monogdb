# Hello Mongodb

A playground app to play with query Mongodb from NodeJs.


## Getting started

To run this example you need node

### Get Node

I recommend using nvm to manage the different versions of node on your machine.

    # From https://github.com/creationix/nvm
    curl -o- https://raw.githubusercontent.com/creationix/nvm/v0.33.5/install.sh | bash
    
    # Install node with nvm
    nvm install v8.1.2
     
### Get Yarn

Browse to the following link and follow the instructions
    
    https://yarnpkg.com/en/docs/install    
    
    
### Check out this repo

    git clone git@github.com:bitstack701/hello-mongodb.git
    cd hello-mongodb
    yarn 
    
    # Run the hello.js 
    yarn hello 

    # See the package.json#scripts block for more things to run.
    
## Dump and restore a collection from Mongodb

    # dump
    ./mongodump -h localhost --port 27017 -d <DatabaseName> -c <Collection> -q '{"creationDate": {"$gte": new Date("2017-10-13T12:10:40.178Z")}}' --archive=<Filename>.gz --gzip 
    
    # restore
    ./mongorestore -h localhost --port 27017 -d <DatabaseName>  --gzip --archive=<Filename>.gz   