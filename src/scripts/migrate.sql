--WIP
CREATE OR REPLACE FUNCTION migrate()
RETURNS void AS $$
DECLARE

auxTable TABLE;
validatedTable TABLE;

BEGIN
    SELECT * into auxTable FROM
    OPENROWSET ('MSDASQL', 'Driver={Microsoft Text Driver (*.txt; *.csv)};DefaultDir={Directory Path of the CSV File};', 
    'SELECT * from yourfile.csv');

    SELECT * INTO validatedTable FROM auxTable A
INNER JOIN validation_table B ON A.Datatype = B.Datatype
INNER JOIN validation_table C ON A.Country = C.Country
INNER JOIN validation_table D ON A.Currency = D.Currency
END; 