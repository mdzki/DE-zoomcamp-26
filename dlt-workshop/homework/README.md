# Homework: Build Your Own dlt Pipeline


## The Challenge

For this homework, build a dlt pipeline that loads NYC taxi trip data from a custom API into DuckDB and then answer some questions using the loaded data.

## Data Source

You'll be working with **NYC Yellow Taxi trip data** from a custom API (not available as a dlt scaffold). This dataset contains records of individual taxi trips in New York City.

| Property | Value |
|----------|-------|
| Base URL | `https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api` |
| Format | Paginated JSON |
| Page Size | 1,000 records per page |
| Pagination | Stop when an empty page is returned |


---

## Questions

Once your pipeline has run successfully, use the methods covered in the workshop to investigate the following:

- **dlt Dashboard**: `dlt pipeline taxi_pipeline show`
- **dlt MCP Server**: Ask the agent questions about your pipeline
- **Marimo Notebook**: Build visualizations and run queries

We challenge you to try out the different methods explored in the workshop when answering these questions to see what works best for you. Feel free to share your thoughts on what worked (or didn't) in your submission!

### Question 1: What is the start date and end date of the dataset?

```
SELECT min(trip_pickup_date_time), max(trip_pickup_date_time) 
FROM taxi_pipeline_dataset.nyc_taxi_data;
```

- ~~2009-01-01 to 2009-01-31~~
- __2009-06-01 to 2009-07-01__
- ~~2024-01-01 to 2024-02-01~~
- ~~2024-06-01 to 2024-07-01~~

### Question 2: What proportion of trips are paid with credit card?
```
SELECT count(case when payment_type= 'Credit' then 1 end)/ count(*)
FROM taxi_pipeline_dataset.nyc_taxi_data;
```
- ~~16.66%~~
- __26.66%__
- ~~36.66%~~
- ~~46.66%~~

### Question 3: What is the total amount of money generated in tips?
```
SELECT sum(tip_amt) FROM taxi_pipeline_dataset.nyc_taxi_data;
```
- ~~$4,063.41~~
- __$6,063.41__
- ~~$8,063.41~~
- ~~$10,063.41~~


### Resources

| Resource | Link |
|----------|------|
| dlt Dashboard Docs | [dlthub.com/docs/general-usage/dashboard](https://dlthub.com/docs/general-usage/dashboard) |
| marimo + dlt Guide | [dlthub.com/docs/general-usage/dataset-access/marimo](https://dlthub.com/docs/general-usage/dataset-access/marimo) |
| dlt Documentation | [dlthub.com/docs](https://dlthub.com/docs) |

---

## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/dlt
- Deadline: See the website
