COPY (SELECT row_to_json(t) FROM (SELECT * FROM airbnb_data.listings) t) 
TO '/var/lib/postgresql/AirbnbData/listings_exported.json';

COPY (SELECT row_to_json(t) FROM (SELECT * FROM airbnb_data.reviews) t) 
TO '/var/lib/postgresql/AirbnbData/reviews_exported.json';