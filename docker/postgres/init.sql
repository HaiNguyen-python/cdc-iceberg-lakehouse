CREATE TABLE users (
    id bigint PRIMARY KEY,
    email text,
    full_name text,
    country text,
    created_at timestamptz,
    updated_at timestamptz
);

CREATE TABLE courses (
    id bigint PRIMARY KEY,
    title text,
    category text,
    price_cents bigint,
    published boolean,
    created_at timestamptz,
    updated_at timestamptz
);

CREATE TABLE enrolments (
    id bigint PRIMARY KEY,
    user_id bigint,
    course_id bigint,
    status text,
    progress_pct bigint,
    enrolled_at timestamptz,
    updated_at timestamptz
);

CREATE TABLE payments (
    id bigint PRIMARY KEY,
    enrolment_id bigint,
    amount_cents bigint,
    currency text,
    status text,
    created_at timestamptz,
    updated_at timestamptz
);

ALTER TABLE users REPLICA IDENTITY FULL;

ALTER TABLE courses REPLICA IDENTITY FULL;

ALTER TABLE enrolments REPLICA IDENTITY FULL;

ALTER TABLE payments REPLICA IDENTITY FULL;
