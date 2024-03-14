-- Dimension Tables
CREATE TABLE dim_province (
    province_id INT PRIMARY KEY,
    province_name VARCHAR(255)
);

CREATE TABLE dim_district (
    district_id INT PRIMARY KEY,
    province_id INT,
    district_name VARCHAR(255),
    FOREIGN KEY (province_id) REFERENCES dim_province(province_id)
);

CREATE TABLE dim_case_table (
    Id INT PRIMARY KEY,
    status_name VARCHAR(255),
    status_detail VARCHAR(255)
);

-- Fact Tables
CREATE TABLE fact_province_daily (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    province_id INT,
    case_id INT,
    date DATE,
    total INT,
    FOREIGN KEY (province_id) REFERENCES dim_province(province_id),
    FOREIGN KEY (case_id) REFERENCES dim_case_table(Id)
);

CREATE TABLE fact_province_monthly (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    province_id INT,
    case_id INT,
    month INT,
    total INT,
    FOREIGN KEY (province_id) REFERENCES dim_province(province_id),
    FOREIGN KEY (case_id) REFERENCES dim_case_table(Id)
);

CREATE TABLE fact_province_yearly (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    province_id INT,
    case_id INT,
    year INT,
    total INT,
    FOREIGN KEY (province_id) REFERENCES dim_province(province_id),
    FOREIGN KEY (case_id) REFERENCES dim_case_table(Id)
);

CREATE TABLE fact_district_monthly (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    district_id INT,
    case_id INT,
    month INT,
    total INT,
    FOREIGN KEY (district_id) REFERENCES dim_district(district_id),
    FOREIGN KEY (case_id) REFERENCES dim_case_table(Id)
);

CREATE TABLE fact_district_yearly (
    Id INT AUTO_INCREMENT PRIMARY KEY,
    district_id INT,
    case_id INT,
    year INT,
    total INT,
    FOREIGN KEY (district_id) REFERENCES dim_district(district_id),
    FOREIGN KEY (case_id) REFERENCES dim_case_table(Id)
);