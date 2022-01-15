from mysql_connection import conn
from datetime import datetime


def getCountries():
    countries_cursor = conn.cursor()
    countries_sql = "SELECT id, country_name FROM countries"
    countries_cursor.execute(countries_sql)
    all_countries = countries_cursor.fetchall()
    conn.commit()
    countries_cursor.close()
    conn.close()

    return all_countries


def getLocations(country_id):
    locations_cursor = conn.cursor()
    locations_sql = "SELECT id, location_url FROM locations WHERE country_id = {} AND location_status = 1 ORDER BY id DESC".format(
        country_id)
    locations_cursor.execute(locations_sql)
    all_locations = locations_cursor.fetchall()
    conn.commit()
    locations_cursor.close()
    conn.close()

    return all_locations


def getScrapeData(country_id):
    location_data_sql = "SELECT scrape_url FROM scrape_data WHERE country_id = {}".format(
        country_id)
    location_data_cursor = conn.cursor(buffered=True)
    location_data_cursor.execute(location_data_sql)
    conn.commit()
    location_data_cursor.close()
    conn.close()

    all_scrape_data = list(location_data_cursor.fetchall())
    scrape_data = [i[0] for i in all_scrape_data]

    return scrape_data


def storeScrapeData(location_id, country_id, tender_link, scrape_text, data_scrape_time):
    scrape_data_sql = "INSERT INTO scrape_data (location_id, country_id, scrape_url, scrape_text, data_scrape_time, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    scrape_data_val = (location_id, country_id, tender_link[:3000], scrape_text[:2048], data_scrape_time, datetime.today(
    ).strftime('%Y-%m-%d %H:%M:%S'), datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    scrape_data_cursor = conn.cursor()
    scrape_data_cursor.execute(scrape_data_sql, scrape_data_val)
    conn.commit()
    scrape_data_cursor.close()
    conn.close()


def storeScrapeLocationData(location_id, country_id, location_http_status_code, location_scrape_time, location_all_links_count, new_location_links):
    location_data_sql = "INSERT INTO scrape_locations (location_id, country_id, location_http_status_code, location_scrape_time, location_all_links_count, location_new_links_count, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    location_data_val = (location_id, country_id, location_http_status_code, location_scrape_time, location_all_links_count,
                         new_location_links, datetime.today().strftime('%Y-%m-%d %H:%M:%S'), datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    location_data_cursor = conn.cursor()
    location_data_cursor.execute(location_data_sql, location_data_val)
    conn.commit()
    location_data_cursor.close()
    conn.close()


def storeScrapeLogs(country_id, total_scrape_time, scrape_locations_no, scrape_all_links, scrape_new_links, scrape_404_count, scrape_5xx_count, scrape_started_at):
    scrape_log_sql = "INSERT INTO scrapes (country_id, scrape_time, scrape_locations_count, scrape_all_links_count, scrape_new_links_count, scrape_404_count, scrape_5xx_count, started_at, ended_at, created_at, updated_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    scrape_log_val = (country_id, total_scrape_time, scrape_locations_no, scrape_all_links, scrape_new_links, scrape_404_count, scrape_5xx_count, scrape_started_at,
                      datetime.today().strftime('%Y-%m-%d %H:%M:%S'), datetime.today().strftime('%Y-%m-%d %H:%M:%S'), datetime.today().strftime('%Y-%m-%d %H:%M:%S'))
    scrape_log_cursor = conn.cursor()
    scrape_log_cursor.execute(scrape_log_sql, scrape_log_val)
    conn.commit()
    scrape_log_cursor.close()
    conn.close()
