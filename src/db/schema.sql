CREATE TABLE locations (
    location_id SERIAL PRIMARY KEY,
    location_name VARCHAR(255) NOT NULL, -- e.g., neighborhood/area
    listed_in_city VARCHAR(255) NOT NULL, -- major city
    CONSTRAINT unique_location_city UNIQUE (location_name, listed_in_city)
);

CREATE TABLE restaurants (
    restaurant_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    address TEXT,
    phone VARCHAR(100),
    location_id INTEGER REFERENCES locations(location_id) ON DELETE SET NULL
);

CREATE TABLE restaurant_metrics_services (
    metric_id SERIAL PRIMARY KEY,
    restaurant_id INTEGER REFERENCES restaurants(restaurant_id) ON DELETE CASCADE,
    online_order BOOLEAN DEFAULT FALSE,
    book_table BOOLEAN DEFAULT FALSE,
    rate NUMERIC(3, 2), -- handles scores like 4.2, 3.8
    votes INTEGER DEFAULT 0,
    rest_type VARCHAR(255),
    cuisines TEXT,
    approx_cost_two INTEGER, -- cost for two people
    menu_item TEXT, -- stores available menu array/text representation
    listed_in_type VARCHAR(100) -- e.g., Buffet, Delivery, Dining
);