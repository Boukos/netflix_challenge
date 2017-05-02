records = load 'TrainingRatings.txt' USING PigStorage(',') as (movieID:int, userID:int, rating:float);

user = GROUP records BY userID;

avg = FOREACH user GENERATE group, AVG(records.rating);

store avg into 'result2';
