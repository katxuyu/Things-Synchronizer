import azure.functions as func
import logging
import redis
import psycopg2


app = func.FunctionApp()


REDIS_HOST = 'things-webhooks.redis.cache.windows.net'
REDIS_PORT = 6379
REDIS_PASS = "YgGJj1TAV3w93ZkVNNJC4NY1w1h3Lv8GjAzCaEO7Ag8="
PG_DBNAME = "postgres"
PG_HOST = "appsmitdb.postgres.database.azure.com"
PG_UN = "packetworx"
PG_PWD = "glpat-z7sF7yWVDn2KR7pbyEtR"
PG_PORT = "5432"

r_reconn_cnt = 0

def pg_connect():
    conn = psycopg2.connect(database=PG_DBNAME,
                            host=PG_HOST,
                            user=PG_UN,
                            password=PG_PWD,
                            port=PG_PORT)
    return conn
   

def set_redis_data(app_name, values):
    # global r_reconn_cnt
    # try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS)
    r.set(app_name,str(values))
    #    r_reconn_cnt = 0 
    # except Exception as e:
    #     if r_reconn_cnt <= 5 and (e == redis.ConnectionError or e == redis.RedisError):
    #         r_reconn_cnt += 1
    #         set_redis_data(app_name,values)


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    try:
        req_body = req.get_json()
        app_name = req_body["application_name"]
    except ValueError as e:
        return func.HttpResponse(f"Request error: {e}", status_code=501)

    try:
        conn = pg_connect()
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM things.view_application_webhooks WHERE application_name = '{app_name}'")
        data = {}

        for rec in cursor.fetchall():
            app_name, base_url, additional_headers, filter_event_data, basic_auth_un, basic_auth_pw = rec
            filter_event_data = filter_event_data if filter_event_data != None else {}
            additional_headers = additional_headers if additional_headers != None else {}
            basic_auth_un = basic_auth_un if basic_auth_un != None else ""
            basic_auth_pw = basic_auth_pw if basic_auth_pw != None else ""
            data[base_url] = {
                "additional_headers": additional_headers,
                "filter_event_data": filter_event_data,
                "basic_auth_un": basic_auth_un,
                "basic_auth_pw": basic_auth_pw
            }
        set_redis_data(app_name, data)
    except Exception as e:
        return func.HttpResponse(f"Internal server error: {e}", status_code=500)
    else:        
        return  func.HttpResponse(
                f"Success {req}",
                status_code=200
        )







    