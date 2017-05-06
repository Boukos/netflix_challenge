records = load 'TrainingRatings.txt' USING PigStorage(',') as (movieID:int, userID:int, rating:float);

// uncomment line to get either overall, movie, or user average
// user = GROUP records BY movieID;
// user = GROUP records BY userID;
user = GROUP records all;

avg = FOREACH user GENERATE group, AVG(records.rating);

store avg into 'result2';
