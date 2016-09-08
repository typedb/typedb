## Visualisation script for Mindmaps Engine

#### Prerequisits

Current MME does not allow CORS so we have to use a php proxy, so you need to install PHP

#### How to run

1. Start mindmaps engine. Make sure it is configured for port `8080`

2. For one-off builds do ```browserify -t vueify -e src/main.js -o static/build.js```, you can also run `watchify`: ```watchify -t vueify -e src/main.js -o static/build.js```

3. Run ```php -S localhost:8000``` in the `static` folder. PHP is needed to proxy MME requests.
