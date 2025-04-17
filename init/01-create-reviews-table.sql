CREATE TABLE IF NOT EXISTS Reviews (
    Id INT PRIMARY KEY,
    ProductId TEXT,
    UserId TEXT,
    ProfileName TEXT,
    HelpfulnessNumerator INT,
    HelpfulnessDenominator INT,
    Score INT,
    Time BIGINT,
    Summary TEXT,
    Text TEXT
);