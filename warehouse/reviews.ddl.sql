CREATE TABLE reviews_data (
    Id INT,
    ProductId VARCHAR(255),
    UserId VARCHAR(255),
    ProfileName VARCHAR(255),
    HelpfulnessNumerator INT,
    HelpfulnessDenominator INT,
    Score INT,
    Summary VARCHAR(255),
    Text TEXT,
    score_normalized DOUBLE PRECISION
);