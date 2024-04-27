CREATE database covid_db;

use covid_db;

CREATE TABLE IF NOT EXISTS covid_db.covid_staging 
(
    Country                            STRING,
    Total_Cases                        DOUBLE,
    New_Cases                          DOUBLE,
    Total_Deaths                       DOUBLE,
    New_Deaths                         DOUBLE,
    Total_Recovered                    DOUBLE,
    Active_Cases                       DOUBLE,
    Serious                            DOUBLE,
    Tot_Cases                          DOUBLE,
    Deaths                             DOUBLE,
    Total_Tests                        DOUBLE,
    Tests                              DOUBLE,
    CASES_per_Test                     DOUBLE,
    Death_in_Closed_Cases              STRING,
    Rank_by_Testing_rate               DOUBLE,
    Rank_by_Death_rate                 DOUBLE,
    Rank_by_Cases_rate                 DOUBLE,
    Rank_by_Death_of_Closed_Cases      DOUBLE
)
ROW FORMAT DELIMITED FIELDS TERMINATED by '\t'
STORED as TEXTFILE
LOCATION '/user/cloudera/ds/COVID_HDFS_LZ'
tblproperties ("skip.header.line.count"="1");


--and creation of table covid_ds_partitioned 
CREATE EXTERNAL TABLE IF NOT EXISTS covid_db.covid_ds_partitioned 
(
    Country 			            STRING,
    Total_Cases   		            DOUBLE,
    New_Cases    		            DOUBLE,
    Total_Deaths                    DOUBLE,
    New_Deaths                      DOUBLE,
    Total_Recovered                 DOUBLE,
    Active_Cases                    DOUBLE,
    Serious			                DOUBLE,
    Tot_Cases                   	DOUBLE,
    Deaths                      	DOUBLE,
    Total_Tests                   	DOUBLE,
    Tests			             	DOUBLE,
    CASES_per_Test                  DOUBLE,
    Death_in_Closed_Cases     	    STRING,
    Rank_by_Testing_rate 		    DOUBLE,
    Rank_by_Death_rate    		    DOUBLE,
    Rank_by_Cases_rate    		    DOUBLE,
    Rank_by_Death_of_Closed_Cases   DOUBLE
)
PARTITIONED BY (COUNTRY_NAME STRING)
STORED AS ORC
LOCATION '/user/cloudera/ds/COVID_HDFS_PARTITIONED';

-----------insert data into Covid_ds_partitioned------------------------

INSERT INTO TABLE covid_db.covid_ds_partitioned PARTITION(COUNTRY_NAME)
SELECT *, Country 
FROM covid_db.covid_staging 
WHERE Country IN (
  'World', 'USA', 'Brazil', 'India', 'Russia', 'South Africa', 'Peru', 'Mexico', 'Colombia', 'Spain',
  'Chile', 'Iran', 'UK', 'Argentina', 'Saudi Arabia', 'Pakistan', 'Bangladesh', 'Italy', 'Turkey',
  'Germany', 'France', 'Iraq', 'Philippines', 'Indonesia', 'Canada', 'Qatar', 'Ecuador', 'Bolivia',
  'Kazakhstan', 'Israel', 'Ukraine', 'Egypt', 'Dominican Republic', 'Sweden', 'China', 'Panama', 'Oman','Curaça'
);

INSERT INTO TABLE covid_db.covid_ds_partitioned PARTITION(COUNTRY_NAME)
SELECT *, Country 
FROM covid_db.covid_staging 
WHERE Country IN (
  'Belgium', 'Kuwait', 'Romania', 'Belarus', 'Guatemala', 'UAE', 'Netherlands', 'Poland', 'Japan',
  'Singapore', 'Portugal', 'Honduras', 'Nigeria', 'Bahrain', 'Morocco', 'Ghana', 'Kyrgyzstan', 'Armenia',
  'Algeria', 'Switzerland', 'Afghanistan', 'Venezuela', 'Uzbekistan', 'Ethiopia', 'Azerbaijan', 'Moldova',
  'Kenya', 'Costa Rica', 'Serbia', 'Nepal', 'Ireland', 'Austria', 'Australia', 'El Salvador', 'Czechia', 'Cameroon','Réunio'
);

INSERT INTO TABLE covid_db.covid_ds_partitioned PARTITION(COUNTRY_NAME)
SELECT *, Country 
FROM covid_db.covid_staging 
WHERE Country IN (
  'Palestine', 'Ivory Coast', 'Bosnia and Herzegovina', 'S. Korea', 'Denmark', 'Bulgaria', 'Madagascar',
  'North Macedonia', 'Sudan', 'Senegal', 'Paraguay', 'Lebanon', 'Zambia', 'Norway', 'DRC', 'Libya',
  'Malaysia', 'Guinea', 'French Guiana', 'Gabon', 'Tajikistan', 'Haiti', 'Albania', 'Greece', 'Finland',
  'Luxembourg', 'Croatia', 'Mauritania', 'Maldives', 'Zimbabwe', 'Djibouti', 'Malawi', 'Hungary', 'Namibia',
  'Equatorial Guinea', 'CAR', 'Hong Kong', 'Nicaragua', 'Montenegro', 'Eswatini', 'Congo'
);

INSERT INTO TABLE covid_db.covid_ds_partitioned PARTITION(COUNTRY_NAME)
SELECT *, Country 
FROM covid_db.covid_staging 
WHERE Country IN (
  'Cuba', 'Thailand', 'Cabo Verde', 'Suriname', 'Somalia', 'Mayotte', 'Mozambique', 'Slovakia', 'Sri Lanka',
  'Rwanda', 'Mali', 'Tunisia', 'Slovenia', 'Lithuania', 'South Sudan', 'Gambia', 'Estonia', 'Guinea-Bissau',
  'Benin', 'Angola', 'Iceland', 'Syria', 'Sierra Leone', 'Yemen', 'Uganda', 'New Zealand', 'Bahamas', 'Malta',
  'Uruguay', 'Jordan', 'Cyprus', 'Aruba', 'Georgia', 'Latvia', 'Botswana', 'Burkina Faso', 'Liberia', 'Togo'
);

INSERT INTO TABLE covid_db.covid_ds_partitioned PARTITION(COUNTRY_NAME)
SELECT *, Country 
FROM covid_db.covid_staging 
WHERE Country IN (
  'Jamaica', 'Niger', 'Andorra', 'Vietnam', 'Lesotho', 'Rأ©union', 'Chad', 'Sao Tome and Principe', 'Guyana',
  'Trinidad and Tobago', 'Diamond Princess', 'San Marino', 'Channel Islands', 'Belize', 'Guadeloupe', 'Tanzania',
  'Taiwan', 'Burundi', 'Comoros', 'Myanmar', 'Faeroe Islands', 'Papua New Guinea', 'Sint Maarten', 'Mauritius',
  'Isle of Man', 'Martinique', 'Turks and Caicos', 'Eritrea', 'Mongolia', 'Cambodia'
);

INSERT INTO TABLE covid_db.covid_ds_partitioned PARTITION(COUNTRY_NAME)
SELECT *, Country 
FROM covid_db.covid_staging 
WHERE Country IN (
  'Gibraltar', 'French Polynesia', 'Cayman Islands', 'Bermuda', 'Barbados', 'Monaco', 'Bhutan', 'Brunei',
  'Seychelles', 'Saint Martin', 'Liechtenstein', 'Antigua and Barbuda', 'St. Vincent Grenadines', 'Macao',
  'Curaأ§a', 'Fiji', 'Saint Lucia', 'Timor-Leste', 'Grenada', 'New Caledonia', 'Laos', 'Dominica',
  'Saint Kitts and Nevis', 'St. Barth', 'Greenland', 'Montserrat', 'Caribbean Netherlands', 'Falkland Islands',
  'Vatican City', 'British Virgin Islands', 'Western Sahara', 'MS Zaandam', 'Saint Pierre Miquelon', 'Anguilla'
);


-----------------------------------------------------------------

CREATE EXTERNAL TABLE covid_db.covid_final_output 
(
 TOP_DEATH 			                STRING,
 TOP_TEST 			                STRING
)
PARTITIONED BY (COUNTRY_NAME STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED by ','
STORED as TEXTFILE
LOCATION '/user/cloudera/ds/COVID_FINAL_OUTPUT';


---------------------------insert data into Covid_final_output---------------------------------------------------
insert into covid_final_output PARTITION(COUNTRY_NAME) 
select deaths,tests,country from covid_ds_partitioned
WHERE Country IN (
  'World', 'USA', 'Brazil', 'India', 'Russia', 'South Africa', 'Peru', 'Mexico', 'Colombia', 'Spain',
  'Chile', 'Iran', 'UK', 'Argentina', 'Saudi Arabia', 'Pakistan', 'Bangladesh', 'Italy', 'Turkey',
  'Germany', 'France', 'Iraq', 'Philippines', 'Indonesia', 'Canada', 'Qatar', 'Ecuador', 'Bolivia',
  'Kazakhstan', 'Israel', 'Ukraine', 'Egypt', 'Dominican Republic', 'Sweden', 'China', 'Panama', 'Oman','Curaça'
);

insert into covid_final_output PARTITION(COUNTRY_NAME) 
select deaths,tests,country from covid_ds_partitioned
WHERE Country IN (
  'Belgium', 'Kuwait', 'Romania', 'Belarus', 'Guatemala', 'UAE', 'Netherlands', 'Poland', 'Japan',
  'Singapore', 'Portugal', 'Honduras', 'Nigeria', 'Bahrain', 'Morocco', 'Ghana', 'Kyrgyzstan', 'Armenia',
  'Algeria', 'Switzerland', 'Afghanistan', 'Venezuela', 'Uzbekistan', 'Ethiopia', 'Azerbaijan', 'Moldova',
  'Kenya', 'Costa Rica', 'Serbia', 'Nepal', 'Ireland', 'Austria', 'Australia', 'El Salvador', 'Czechia', 'Cameroon','Réunio'
);

insert into covid_final_output PARTITION(COUNTRY_NAME) 
select deaths,tests,country from covid_ds_partitioned
WHERE Country IN (
  'Palestine', 'Ivory Coast', 'Bosnia and Herzegovina', 'S. Korea', 'Denmark', 'Bulgaria', 'Madagascar',
  'North Macedonia', 'Sudan', 'Senegal', 'Paraguay', 'Lebanon', 'Zambia', 'Norway', 'DRC', 'Libya',
  'Malaysia', 'Guinea', 'French Guiana', 'Gabon', 'Tajikistan', 'Haiti', 'Albania', 'Greece', 'Finland',
  'Luxembourg', 'Croatia', 'Mauritania', 'Maldives', 'Zimbabwe', 'Djibouti', 'Malawi', 'Hungary', 'Namibia',
  'Equatorial Guinea', 'CAR', 'Hong Kong', 'Nicaragua', 'Montenegro', 'Eswatini', 'Congo'
);

insert into covid_final_output PARTITION(COUNTRY_NAME) 
select deaths,tests,country from covid_ds_partitioned
WHERE Country IN (
  'Cuba', 'Thailand', 'Cabo Verde', 'Suriname', 'Somalia', 'Mayotte', 'Mozambique', 'Slovakia', 'Sri Lanka',
  'Rwanda', 'Mali', 'Tunisia', 'Slovenia', 'Lithuania', 'South Sudan', 'Gambia', 'Estonia', 'Guinea-Bissau',
  'Benin', 'Angola', 'Iceland', 'Syria', 'Sierra Leone', 'Yemen', 'Uganda', 'New Zealand', 'Bahamas', 'Malta',
  'Uruguay', 'Jordan', 'Cyprus', 'Aruba', 'Georgia', 'Latvia', 'Botswana', 'Burkina Faso', 'Liberia', 'Togo'
);

insert into covid_final_output PARTITION(COUNTRY_NAME) 
select deaths,tests,country from covid_ds_partitioned
WHERE Country IN (
  'Jamaica', 'Niger', 'Andorra', 'Vietnam', 'Lesotho', 'Rأ©union', 'Chad', 'Sao Tome and Principe', 'Guyana',
  'Trinidad and Tobago', 'Diamond Princess', 'San Marino', 'Channel Islands', 'Belize', 'Guadeloupe', 'Tanzania',
  'Taiwan', 'Burundi', 'Comoros', 'Myanmar', 'Faeroe Islands', 'Papua New Guinea', 'Sint Maarten', 'Mauritius',
  'Isle of Man', 'Martinique', 'Turks and Caicos', 'Eritrea', 'Mongolia', 'Cambodia'
);

insert into covid_final_output PARTITION(COUNTRY_NAME) 
select deaths,tests,country from covid_ds_partitioned
WHERE Country IN (
  'Gibraltar', 'French Polynesia', 'Cayman Islands', 'Bermuda', 'Barbados', 'Monaco', 'Bhutan', 'Brunei',
  'Seychelles', 'Saint Martin', 'Liechtenstein', 'Antigua and Barbuda', 'St. Vincent Grenadines', 'Macao',
  'Curaأ§a', 'Fiji', 'Saint Lucia', 'Timor-Leste', 'Grenada', 'New Caledonia', 'Laos', 'Dominica',
  'Saint Kitts and Nevis', 'St. Barth', 'Greenland', 'Montserrat', 'Caribbean Netherlands', 'Falkland Islands',
  'Vatican City', 'British Virgin Islands', 'Western Sahara', 'MS Zaandam', 'Saint Pierre Miquelon', 'Anguilla'
);
