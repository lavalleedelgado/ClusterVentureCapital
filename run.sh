#!/bin/bash

# Patrick Lavallee Delgado
# Department of Computer Science
# University of Chicago
# December 2019

################################################################################
# Run ClusterVentureCapital.
################################################################################

# On the web server, navigate to the working directory.
cd ~/ClusterVentureCapital/web_app

# Install any missing dependencies.
npm install express
npm install mustache
npm install hbase-rpc-client

# Run the application.
node web_app.js

