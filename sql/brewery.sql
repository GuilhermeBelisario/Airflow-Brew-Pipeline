
SET statement_timeout = 0;
SET lock_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET default_tablespace = '';
SET default_with_oids = false;



-- Adicionando extensão para gerar UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Criação da tabela dim_brewery_type
CREATE TABLE IF NOT EXISTS dim_brewery_type (
    type_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    brewery_type VARCHAR(50) NOT NULL
);

-- Criação da tabela dim_brewery 
CREATE TABLE IF NOT EXISTS dim_brewery (
    brewery_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    brewery_type_id UUID REFERENCES dim_brewery_type(type_id),
    phone VARCHAR(50),
    is_phone_missing CHAR(1),
    fixed_phone VARCHAR(50),
    website_url VARCHAR(255)
);

-- Criação da tabela dim_location
CREATE TABLE IF NOT EXISTS dim_location (
    location_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    brewery_id UUID REFERENCES dim_brewery(brewery_id),
    full_address VARCHAR(255),
    address_1 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    zip_code VARCHAR(20),
    country VARCHAR(100),
    longitude FLOAT,
    latitude FLOAT,
    has_location CHAR(1),
    
);

-- Criação da tabela fact_brewery_operations
CREATE TABLE IF NOT EXISTS fact_brewery_operations (
    operation_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    brewery_id UUID REFERENCES dim_brewery(brewery_id),
    location_id UUID REFERENCES dim_location(location_id),
    metric_date DATE NOT NULL,
    data_de_processamento DATE,
    created_year INT,
    created_month INT,
    origem_do_dado VARCHAR(100) NOT NULL,
    formato_na_origem VARCHAR(50),
    pipeline_vinculado VARCHAR(100) NOT NULL,
    nome_do_arquivo_original VARCHAR(100) NOT NULL
);