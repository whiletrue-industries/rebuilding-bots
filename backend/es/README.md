## Quick start with elasticsearch on local dev env

1. Make sure you have `docker` and `docker-compose` installed (docker compose comes built in with docker in its latest versions)

2. Inside the folder 'backend/es/'...

  a. Run `docker compose up` or `docker-compose up` to start the elasticsearch container. You will see setup logs in the terminal (first time would take a while to download the image and start the container).
  b. To run in detached mode, run `docker compose up -d` or `docker-compose up -d` - this will start the container in the background.

3. Run `curl --cacert certs/ca/ca.crt -u elastic:elastic123 https://localhost:9200` - you should see the default elasticsearch response if everything is setup correctly.

4. Set up python dependencies: `pip install -r requirements.txt`

5. Set up environment variables in your `.env` file:

   **For Staging (default):**
   ```env
   ES_HOST_STAGING=https://localhost:9200
   ES_USERNAME_STAGING=elastic
   ES_PASSWORD_STAGING=elastic123
   OPENAI_API_KEY_STAGING=your_openai_api_key_here
   ```

   **For Production:**
   ```env
   ES_HOST_PRODUCTION=https://your-prod-es-host:9200
   ES_USERNAME_PRODUCTION=your_prod_username
   ES_PASSWORD_PRODUCTION=your_prod_password
   OPENAI_API_KEY_PRODUCTION=your_production_openai_api_key
   ```

## To try out loading data into elasticsearch:

Inside the folder 'backend/es/'...

1. Run `python demo-load-data-to-es.py [production|staging]`.
   This will load the `takanon` data files from the local repo.
   You can change the source location of the data, as well as chunking parameters in the script itself.
   
   **Examples:**
   ```bash
   # Load data to staging (default)
   python demo-load-data-to-es.py
   
   # Load data to production
   python demo-load-data-to-es.py production
   ```

## To try out querying the data from elasticsearch:

Inside the folder 'backend/es/'...

1. Run `python demo-query-es.py <query> [production|staging]`.
   This will query the elasticsearch instance for the data loaded in the previous step.
   The first argument is the query string, and the second (optional) argument is the environment.

   **Examples:**
   ```bash
   # Query staging (default)
   python demo-query-es.py "דיון מהיר"
   
   # Query production
   python demo-query-es.py "דיון מהיר" production
   ```

   **Sample output:**
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
   
