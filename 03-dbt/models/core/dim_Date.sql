{{ config(materialized='table')}}

-- DECLARE StartDate DATE DEFAULT PARSE_DATE("%F",'2018-01-01');
-- DECLARE EndDate DATE DEFAULT PARSE_DATE("%F", '2025-12-31');
-- DECLARE NrDays DEFAULT (SELECT DATE_DIFF(EndDate, StartDate, DAY));

WITH s AS
(
    SELECT 0 AS i
    UNION ALL
    SELECT 0 AS i
    UNION ALL
    SELECT 0 AS i
    UNION ALL
    SELECT 0 AS i
),
p AS
(
    SELECT 0 AS i
    FROM s AS a
    CROSS JOIN s AS b
    CROSS JOIN s AS c
    CROSS JOIN s AS d
    CROSS JOIN s AS e
    CROSS JOIN s AS f
),
n AS
(
    SELECT ROW_NUMBER() OVER(ORDER BY (SELECT null)) AS i
    FROM p
)
dates AS 
(
SELECT 
    DATE_ADD(StartDate, INTERVAL i - 1 DAY) AS CurrentDate 
FROM n 
WHERE i <= DATE_DIFF(DATE {{ var('EndDate') }}, DATE {{ var('StartDate') }}, DAY) + 1
)
SELECT EXTRACT(YEAR FROM CurrentDate) * 10000 + EXTRACT(MONTH FROM CurrentDate) * 100 + EXTRACT(DAY FROM CurrentDate) AS DateKey,
      CurrentDate AS DATE,
      EXTRACT(DAY FROM CurrentDate) AS Day,
      EXTRACT(DAYOFWEEK FROM CurrentDate) AS WEEKDAY,
      CASE EXTRACT(DAYOFWEEK FROM CurrentDate)
                        WHEN 1 THEN 'Sunday'
                        WHEN 2 THEN 'Monday'
                        WHEN 3 THEN 'Tuesday'
                        WHEN 4 THEN 'Wednesday'
                        WHEN 5 THEN 'Thursday'
                        WHEN 6 THEN 'Friday'
                        WHEN 7 THEN 'Saturday'
                    END AS WeekDayName,
    
      CASE EXTRACT(DAYOFWEEK FROM CurrentDate)
                        WHEN 1 THEN 'Sun'
                        WHEN 2 THEN 'Mon'
                        WHEN 3 THEN 'Tue'
                        WHEN 4 THEN 'Wed'
                        WHEN 5 THEN 'Thu'
                        WHEN 6 THEN 'Fri'
                        WHEN 7 THEN 'Sat'
                    END AS WeekDayName_Short,
      EXTRACT(DAYOFYEAR FROM CurrentDate) AS DayOfYear,
      EXTRACT(WEEK FROM CurrentDate) AS WeekOfYear,
      EXTRACT(MONTH FROM CurrentDate) AS Month,
      CASE EXTRACT(MONTH FROM CurrentDate)
                        WHEN 1 THEN "January"
                        WHEN 2 THEN "February"
                        WHEN 3 THEN "March"
                        WHEN 4 THEN "April"
                        WHEN 5 THEN "May"
                        WHEN 6 THEN "June"
                        WHEN 7 THEN "July"
                        WHEN 8 THEN "August"
                        WHEN 9 THEN "September"
                        WHEN 10 THEN "October"
                        WHEN 11 THEN "November" 
                        WHEN 12 THEN "December"                        
                        END AS MonthName,
      CASE EXTRACT(MONTH FROM CurrentDate)
                        WHEN 1 THEN "Jan"
                        WHEN 2 THEN "Feb"
                        WHEN 3 THEN "Mar"
                        WHEN 4 THEN "Apr"
                        WHEN 5 THEN "May"
                        WHEN 6 THEN "Jun"
                        WHEN 7 THEN "Jul"
                        WHEN 8 THEN "Aug"
                        WHEN 9 THEN "Sep"
                        WHEN 10 THEN "Oct"
                        WHEN 11 THEN "Nov" 
                        WHEN 12 THEN "Dec"                        
                        END AS MonthName_Short,
      EXTRACT(QUARTER FROM CurrentDate) AS Quarter,
      CASE EXTRACT(QUARTER FROM CurrentDate)
                        WHEN 1 THEN 'First'
                        WHEN 2 THEN 'Second'
                        WHEN 3 THEN 'Third'
                        WHEN 4 THEN 'Fourth'
                        END AS QuarterName,
      EXTRACT(YEAR FROM CurrentDate) Year,
      RIGHT("0" || CAST (EXTRACT(MONTH FROM CurrentDate) AS STRING), 2) || CAST (EXTRACT(YEAR FROM CurrentDate) AS STRING) AS MMYYYY,
      CAST (EXTRACT(YEAR FROM CurrentDate) AS STRING) || RIGHT("0" || CAST (EXTRACT(MONTH FROM CurrentDate) AS STRING), 2) AS YYYYMM
FROM dates
      
