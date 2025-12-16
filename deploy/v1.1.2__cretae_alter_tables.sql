CREATE TABLE IF NOT EXISTS CUSTOMER (
    CUSTOMER_ID INT AUTOINCREMENT PRIMARY KEY,
    CUSTOMER_NAME VARCHAR(100) NOT NULL,
    EMAIL VARCHAR(150),
    PHONE_NUMBER VARCHAR(15),
    GENDER VARCHAR(10),
    DATE_OF_BIRTH DATE,
    CITY VARCHAR(50),
    STATE VARCHAR(50),
    COUNTRY VARCHAR(50),
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO CUSTOMER
(CUSTOMER_NAME, EMAIL, PHONE_NUMBER, GENDER, DATE_OF_BIRTH, CITY, STATE, COUNTRY)
VALUES
('Ramesh Kumar', 'ramesh.kumar@gmail.com', '9876543210', 'Male', '1990-05-12', 'Bangalore', 'Karnataka', 'India'),
('Anita Sharma', 'anita.sharma@gmail.com', '9123456789', 'Female', '1988-08-25', 'Delhi', 'Delhi', 'India'),
('Vijay Patel', 'vijay.patel@gmail.com', '9988776655', 'Male', '1992-03-18', 'Ahmedabad', 'Gujarat', 'India'),
('Neha reddy', 'neha.verma@gmail.com', '9090909090', 'Female', '1995-11-02', 'Mumbai', 'Maharashtra', 'India'),
('Arjun reddy', 'arjun.reddy@gmail.com', '9555443322', 'Male', '1991-07-09', 'Hyderabad', 'Telangana', 'India');
