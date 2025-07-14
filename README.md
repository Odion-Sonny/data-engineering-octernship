# DuckMart User Segmentation System

## Implementation Overview

This project implements a scalable user segmentation system for DuckMart, a fictional e-commerce platform. The system allows for flexible user segmentation based on user attributes and behavioral events through a REST API.

### Key Features
- **Dummy Data Generation**: 10,000 users and 50,000 events with realistic attributes
- **DuckDB Integration**: Efficient data storage and querying
- **Flexible Segmentation API**: JSON-based segmentation criteria with SQL generation
- **Multiple Filter Types**: User attributes, event-based filters, and time-range filtering
- **Performance Optimized**: Indexed queries for fast segmentation

### Project Structure
```
├── generate_dummy_data.py      # Generate sample data
├── database_setup.py           # Database schema and data loading
├── segmentation_queries.py     # Example segmentation queries
├── segmentation_api.py         # FastAPI backend service
├── test_api.py                 # API testing script
├── requirements.txt            # Python dependencies
├── user_attributes.csv         # Generated user data
├── user_events.csv            # Generated event data
└── duckmart.db                # DuckDB database file
```

## Setup Instructions

### Prerequisites
- Python 3.8+
- pip package manager

### Installation
1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Generate dummy data:
   ```bash
   python generate_dummy_data.py
   ```

4. Set up database and load data:
   ```bash
   python database_setup.py
   ```

5. Run example segmentation queries:
   ```bash
   python segmentation_queries.py
   ```

6. Start the API server:
   ```bash
   python segmentation_api.py
   ```

7. Test the API (in another terminal):
   ```bash
   python test_api.py
   ```

## Database Schema

### User Attributes Table
```sql
CREATE TABLE user_attributes (
    user_id INTEGER PRIMARY KEY,
    name VARCHAR,
    age INTEGER,
    gender VARCHAR,
    location VARCHAR,
    signup_date DATE,
    subscription_plan VARCHAR,
    device_type VARCHAR
)
```

### User Events Table
```sql
CREATE TABLE user_events (
    user_id INTEGER,
    event_name VARCHAR,
    timestamp TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES user_attributes(user_id)
)
```

## Required Segmentation Queries

### 1. Age-Based Segmentation (25-34 years)
```sql
SELECT user_id, name, age 
FROM user_attributes 
WHERE age >= 25 AND age <= 34
ORDER BY user_id
```
**Result**: 1,915 users in this age group

### 2. Location + Event Segmentation (California + LOGIN)
```sql
SELECT DISTINCT ua.user_id, ua.name, ua.location
FROM user_attributes ua
INNER JOIN user_events ue ON ua.user_id = ue.user_id
WHERE ua.location = 'California' 
AND ue.event_name = 'LOGIN'
ORDER BY ua.user_id
```
**Result**: 390 California users who have logged in

## Segmentation API Specification

### JSON Payload Structure

The API accepts segmentation criteria in the following JSON format:

```json
{
    "user_filters": [
        {
            "field": "age",
            "operator": "gte", 
            "value": 25
        }
    ],
    "event_filters": [
        {
            "event_name": "LOGIN",
            "operator": "gte",
            "count": 1,
            "time_range_days": 30
        }
    ],
    "logic_operator": "AND",
    "limit": 1000
}
```

### Supported Operators

#### User Filter Operators
- `eq`: equals
- `ne`: not equals  
- `gt`: greater than
- `gte`: greater than or equal
- `lt`: less than
- `lte`: less than or equal
- `in`: value in list
- `not_in`: value not in list
- `like`: pattern matching

#### Event Filter Operators
- `eq`: exact count
- `ne`: not equal count
- `gt`: greater than count
- `gte`: greater than or equal count
- `lt`: less than count
- `lte`: less than or equal count

### API Endpoints

#### POST /segment
Main segmentation endpoint that accepts JSON criteria and returns user IDs.

**Request Example**:
```json
{
    "user_filters": [
        {"field": "location", "operator": "eq", "value": "California"}
    ],
    "event_filters": [
        {"event_name": "LOGIN", "operator": "gte", "count": 1}
    ],
    "logic_operator": "AND"
}
```

**Response Example**:
```json
{
    "user_ids": [25, 32, 54, 63, 81, ...],
    "total_count": 390,
    "filters_applied": {
        "user_filters": [...],
        "event_filters": [...], 
        "logic_operator": "AND"
    }
}
```

#### GET /examples
Returns example JSON payloads for common segmentation scenarios.

#### GET /health
Health check endpoint.

### Example Use Cases

1. **Age Targeting**: Users aged 25-34
2. **Geographic + Behavioral**: California users who logged in
3. **Subscription Targeting**: Premium users with recent purchases
4. **Cart Abandonment**: Users who added items but never purchased
5. **Device Segmentation**: Mobile users with specific behaviors

## Performance Considerations

- **Database Indexes**: Created on frequently queried fields (user_id, age, location, event_name)
- **Query Optimization**: Efficient JOIN operations and filtering
- **Limit Controls**: Configurable result limits to prevent large result sets
- **Connection Management**: Proper database connection handling

## Design Decisions

1. **DuckDB Choice**: Fast analytical queries, embedded database, excellent for read-heavy workloads
2. **FastAPI Framework**: Modern, fast, automatic API documentation
3. **Pydantic Validation**: Type safety and request validation
4. **Flexible JSON Schema**: Extensible design for future filter types
5. **SQL Generation**: Dynamic SQL building with injection protection

## Testing

The project includes comprehensive testing:
- **Data Generation**: Realistic sample data with proper distributions
- **Query Validation**: Verification of segmentation results
- **API Testing**: Automated tests for various segmentation scenarios
- **Performance Testing**: Query execution time monitoring

## Assumptions & Limitations

- User IDs are unique integers
- Event timestamps are stored in UTC
- Maximum result limit of 1000 users per request (configurable)
- Field names are validated to prevent SQL injection
- Time-based filters use days as the minimum unit

---

## Houseware

### Company information 

Houseware's vision is to empower the next generation of knowledge workers by putting the data warehouse in their hands, in the language they speak. Houseware is purpose-built for the Data Cloud’s untouched creators, empowering internal apps across organizations. 

### Why participate in an Octernship with Houseware

Houseware is changing the way the data warehouse is leveraged, and we want you to help build Houseware! Our team came together to answer the singular question, "how can we flip the value of the data warehouse to the ones who really need it, to the ones who drive decisions". 

In this role, you'll have the opportunity to work as a Data engineer with the Houseware team on multiple customer-facing projects. You'd be involved with delivering the data platform for the end user, while taking complete ownership of engineering challenges - this would include communicating with the stakeholders, setting the right expectations, and ensuring top quality for the code & the product being shipped.

### Octernship role description

We're looking for data engineers to join the Houseware team. 

We are hell-bent on building a forward-looking product, something that constantly pushes us to think by first principles and question assumptions, building a team that is agile in adapting and ever curious. While fast-paced execution is one of the prerequisites in this role, equally important is the ability to pause and take stock of where product/engineering is heading from a long-term perspective. Your initiative is another thing that we would expect to shine through here, as you continuously navigate through ambiguous waters while working with vigor on open-ended questions - all to solve problems for and empathize with the end users.

| Octernship info  | Timelines and Stipend |
| ------------- | ------------- |
| Assignment Deadline  | 26 March 2023  |
| Octernship Duration  | 3-6 Months  |
| Monthly Stipend  | $600 USD  |

### Recommended qualifications

- You have a solid problem-solving framework.
- You are well-versed with the Modern Data Stack, and have worked with Cloud Data Warehouses before
- You have prior experience writing backend systems, and are proficient in SQL/dbt.

### Eligibility

To participate, you must be:

* A [verified student](https://education.github.com/discount_requests/pack_application) on Global Campus

* 18 years or older

* Active contributor on GitHub (monthly)

# Assignment

## Segment users on DuckMart!

### Task instructions

You have been given a task to segment the user audience for a fictional online service called "DuckMart". You have to design and implement a backend service that allows for segmenting the user audience based on user attributes and user events.

As part of this activity, you'll have to do the following
- Dummy data generation: Create dummy data using tools like Mockaroo
- Data transformation: Write a Python script to transform the data from the CSV files into a format suitable for loading into the database.
- Data loading: You are required to load the transformed data into DuckDB

Database Schema: The following are the requirements for the database schema:

- User Attributes: User ID, Name, Age, Gender, Location, Signup Date, Subscription Plan, Device Type.
- User Events: User ID, Event Name, Timestamp.

A few examples of events are "PURCHASE_MADE" or "ADDED_TO_CART".

Query Requirements: The following are the requirements for the queries:

- Segment users by age groups: Create a segment of users in the age range 25-34 years and list out the user IDs of all such users.
- Segment users by location and events: Create a segment of users whose location="California" and have logged in to the product at least once(event_name='LOGIN') and list out the User IDs of all such users.

You are then required to write out a backend API endpoint that can scale to any kind of "Segmentation usecase" like the two examples mentioned above. Building on top of the mentioned data schema(Users, Events), the consumer of this API should be able to specify the segmentation criteria in a JSON-like format and the backend API should be able to convert it into the relevant SQL. Please specify what the spec for the JSON-like payload looks like.

### Task Expectations

You will be evaluated based on the following criteria:
- Correctness and completeness of the implementation.
- The JSON spec that powers the "Segmentation API"
- Performance and scalability of the implementation.
- Quality of the SQL queries and their optimization.
- Quality of the code and documentation.
- Ability to explain and justify design decisions.

### Task submission

Students are expected to use the [GitHub Flow](https://docs.github.com/en/get-started/quickstart/github-flow) when working on their project. 

1. Please push your final code changes to your main branch
2. Please add your instructions/assumptions/any other remarks in the beginning of the Readme file and the reviewers will take a look
3. The PR created called Feedback will be used for sharing any feedback/asking questions by the reviewers, please make sure you do not close the Feedback PR.
4. The assignment will be automatically submitted on the "Assignment Deadline" date -- you don't need to do anything apart from what is mentioned above.
5. Using GitHub Issues to ask any relevant questions regarding the project


