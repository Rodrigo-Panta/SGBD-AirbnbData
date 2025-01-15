-- Cria o Schema
CREATE SCHEMA IF NOT EXISTS airbnb_data;

-- Cria e a popula a tabela de listings
DROP TABLE IF EXISTS airbnb_data.listings;

CREATE TABLE IF NOT EXISTS airbnb_data.listings (
    listing_id INT,
    name VARCHAR(385),
    host_id INT,
    host_since DATE,
    host_location VARCHAR(439),
    host_response_time VARCHAR(18),
    host_response_rate DECIMAL(10,2),
    host_acceptance_rate DECIMAL(10,2),
    host_is_superhost BOOLEAN,
    host_total_listings_count DECIMAL(10,2),
    host_has_profile_pic BOOLEAN,
    host_identity_verified BOOLEAN,
    neighbourhood VARCHAR(26),
    district VARCHAR(13),
    city VARCHAR(14),
    latitude DECIMAL(20,9),
    longitude DECIMAL(20,9),
    property_type VARCHAR(34),
    room_type VARCHAR(12),
    accommodates INT,
    bedrooms INT,
    amenities VARCHAR(2858),
    price DECIMAL(10,2),
    minimum_nights INT,
    maximum_nights INT,
    review_scores_rating DECIMAL(10,2),
    review_scores_accuracy DECIMAL(10,2),
    review_scores_cleanliness DECIMAL(10,2),
    review_scores_checkin DECIMAL(10,2),
    review_scores_communication DECIMAL(10,2),
    review_scores_location DECIMAL(10,2),
    review_scores_value DECIMAL(10,2),
    instant_bookable BOOLEAN
);

COPY airbnb_data.listings (
    listing_id,
    name,
    host_id,
    host_since,
    host_location,
    host_response_time,
    host_response_rate,
    host_acceptance_rate,
    host_is_superhost,
    host_total_listings_count,
    host_has_profile_pic,
    host_identity_verified,
    neighbourhood,
    district,
    city,
    latitude,
    longitude,
    property_type,
    room_type,
    accommodates,
    bedrooms,
    amenities,
    price,
    minimum_nights,
    maximum_nights,
    review_scores_rating,
    review_scores_accuracy,
    review_scores_cleanliness,
    review_scores_checkin,
    review_scores_communication,
    review_scores_location,
    review_scores_value,
    instant_bookable
)
FROM '/var/lib/postgresql/AirbnbData/ListingsFiltered.csv'
DELIMITER ','
CSV HEADER;

-- Cria e popula a tabela reviews
DROP TABLE IF EXISTS airbnb_data.reviews;

CREATE TABLE IF NOT EXISTS airbnb_data.reviews (
    listing_id INT,
    review_id INT,
    date DATE,
    reviewer_id INT
);

COPY airbnb_data.reviews (
    listing_id,
    review_id,
    date,
    reviewer_id
)
FROM '/var/lib/postgresql/AirbnbData/Reviews.csv'
DELIMITER ','
CSV HEADER;