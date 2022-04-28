# Analytics of Train Delays of UK Rail

- [Preface](#preface)
- [Introduction](#introduction)
- [Dataset description](#dataset-description)
- [Problem statement](#problem-statement)
- [Requirements](#requirements)
- [Solution](#solutiob)
    - [Technology stack](#technologies)
    - [Infrastructure as a code](#iac)
    - [Cloud infrastructure](#cloud)
    - [Data pipeline orchestration and scheduling](#orchestration)
    - [Data modelling](#datamodelling)
    - [Reporting](#report)
- [Future development](#todo)

## Preface
This repository contains the final project for the Data Engineering Zoomcamp (github.com/DataTalksClub/data-engineering-zoomcamp) - a free course organized by the DataTalks.Club (datatalks.club) community.
Course tutors: Victoria Perez Mola linkedin.com/in/victoriaperezmola, Sejal Vaidya linkedin.com/in/vaidyasejal, Ankush Khanna linkedin.com/in/ankushkhanna2, Alexey Grigorev linkedin.com/in/agrigorev. The project covers main data engineering skills taught in the course: Infrastructure as code, containerization, data lake, ETL/ELT, pipeline orchestration and scheduling, batch processing, data warehousing, reporting.

## Introduction
It was initially intended that the data set would consist of multiple file with record count at least on a level of thousands. Thus, a monthly load can be simulated with an amount of data high enough to consider ELT instead of ETL. After thorough review of the available free data sets, UK passenger train delays data set from NetwokRail was chosen https://www.networkrail.co.uk/who-we-are/transparency-and-ethics/transparency/open-data-feeds/.

## Dataset description


Data and Attributes glossary:
https://www.networkrail.co.uk/who-we-are/transparency-and-ethics/transparency/open-data-feeds/

Stations attreibutes:
https://dataportal.orr.gov.uk/statistics/infrastructure-and-emissions/rail-infrastructure-and-assets

KPIs:

Incident_JPIP_Category vs. SUM Minutes (Heatmap)
 
Operator name vs. 
