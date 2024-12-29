## Quick start with elasticsearch on local dev env

1. Make sure you have `docker` and `docker-compose` installed (docker compose comes built in with docker in its latest versions)

2. Inside the folder 'backend/es/'...

  a. Run `docker compose up` or `docker-compose up` to start the elasticsearch container. You will see setup logs in the terminal (first time would take a while to download the image and start the container).
  b. To run in detached mode, run `docker compose up -d` or `docker-compose up -d` - this will start the container in the background.

3. Run `curl --cacert certs/ca/ca.crt -u elastic:elastic123 https://localhost:9200` - you should see the default elasticsearch response if everything is setup correctly.

4. Set up python dependencies: `pip install -r requirements.txt`

5. Ensure that in the root .env file, OPENAI_API_KEY is set to a valid API key

## To try out loading data into elasticsearch:

Inside the folder 'backend/es/'...

1. Run `python demo-load-data-to-es.py`.
   This will load the `takanon` data files from the local repo.
   You can change the source location of the data, as well as chunking parameters in the script itself.

## To try out querying the data from elasticsearch:

Inside the folder 'backend/es/'...

1. Run `python demo-query-es.py`.
   This will query the elasticsearch instance for the data loaded in the previous step.
   The only command line argument is the query string:

   ```
   $ python demo-query-es.py "דיון מהיר"
      2.25: תקנון הכנסת_61                   [**דיון מהיר והצעה דחופה בנושא דומה**]
      1.76: תקנון הכנסת_60                   [**דיונים מהירים בוועדה**]
      1.04: תקנון הכנסת_54                   [**הצעה דחופה לסדר היום**]
      0.78: תקנון הכנסת_117                  [**עיכוב בעבודת ועדה**]
      0.68: תקנון הכנסת_45                   [**הנוהל בדיון בהשתתפות ראש הממשלה לפיסעיף 42 לחוק־יסוד: הממשלה**]
      0.64: תקנון הכנסת_115                  [**דיון מחדש**]
      0.60: תקנון הכנסת_20                   [**פתיחת מושב הכנסת**]
   ```
   The output is a list of documents with their scores, ids and first line of their content.
   
