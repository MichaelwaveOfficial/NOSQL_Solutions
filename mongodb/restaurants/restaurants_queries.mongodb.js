
const db_name = 'restaurants'

// 1. Write a MongoDB query to display all the documents in the collection restaurants.

db.getCollection(db_name).find();

// 2. Write a MongoDB query to display the fields restaurant_id, name, borough and cuisine for all the documents in the collection restaurant.

db.getCollection(db_name).find(
    {},
    {
        _id : 1, 
        name : 1,
        borough : 1,
        cuisine : 1
    }
);

// 3. Write a MongoDB query to display the fields restaurant_id, name, borough and cuisine, but exclude the field _id for all the documents in the collection restaurant.

db.getCollection(db_name).find(
    {},
    {
        _id : 0, 
        name : 1,
        borough : 1,
        cuisine : 1
    }
);

// 4. Write a MongoDB query to display the fields restaurant_id, name, borough and zip code, but exclude the field _id for all the documents in the collection restaurant.

db.getCollection(db_name).find(
    {},
    {
        _id : 0, 
        name : 1,
        borough : 1,
        cuisine : 1,
        address : { zipcode : 1 }
    }
);

// 5. Write a MongoDB query to display all the restaurant which is in the borough Bronx.

db.getCollection(db_name).find(
    {},
    {
        _id : 0, 
        name : 1, 
        borough : 'Bronx'
    }
);

// 6. Write a MongoDB query to display the first 5 restaurant which is in the borough Bronx.

db.getCollection(db_name).find(
    {},
    {
        _id : 0, 
        name : 1, 
        borough : 'Bronx'
    }
).limit(5);

// 7. Write a MongoDB query to display the next 5 restaurants after skipping first 5 which are in the borough Bronx.

db.getCollection(db_name).find(
    {},
    {
        _id : 0, 
        name : 1, 
        borough : 'Bronx'
    }
).skip(5);

// 8. Write a MongoDB query to find the restaurants who achieved a score more than 90.

const pass_score = 90

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 0, 
            name : 1, 
            total_score : { $sum : '$grades.score' },
        } 
    }, 
    {
        $match : {
            total_score : { $gt : pass_score},
        }
    }
]);

// 9. Write a MongoDB query to find the restaurants that achieved a score, more than 80 but less than 100.

const max_score = 100
const min_score = 80

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 0, 
            name : 1, 
            total_score : { $sum : '$grades.score' },
        } 
    }, 
    {
        $match : {
            total_score : { $gte : min_score, $lte : max_score}
        }
    }
]);

// 10. Write a MongoDB query to find the restaurants which locate in latitude value less than -95.754168. 

const target_latitude =  -95.754168

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 0, 
            name : 1,
            address : { $arrayElemAt : [ '$address.coord', 0 ] }
        }
    },
    {
        $match : {
            address : { $gte : target_latitude }
        }
    }
]);

// 11. Write a MongoDB query to find the restaurants that do not prepare any cuisine of 'American' and their grade score more than 70 and latitude less than -65.754168.

const target_latitude_2 = -65.754168

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 0, 
            name : 1, 
            cuisine : 1,
            address : { $arrayElemAt : [ '$address.coord', 0] }
        }
    },
    {
        $match : {
            address : { $gte : target_latitude_2 }, 
            cuisine : { $ne : 'American' }
        }
    }
]);

// 12. Write a MongoDB query to find the restaurants which do not prepare any cuisine of 'American' and achieved a score more than 70 and not located in the longitude less than -65.754168.
//Note : Do this query without using $and operator.

const target_latitude_2 = -65.754168
const target_score = 70

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 0, 
            name : 1, 
            cuisine : 1,
            address : { $arrayElemAt : [ '$address.coord', 0] },
            total_score : { $sum : '$grades.score' },
        }
    }, 
    {
        $match : {
            address : { $gte : target_latitude_2 }, 
            cuisine : { $ne : 'American' },
            total_score : { $gt : target_score }
        }
    }
]);

// 13. Write a MongoDB query to find the restaurants which do not prepare any cuisine of 'American ' and achieved a grade point 'A' not belongs to the borough Brooklyn.
// The document must be displayed according to the cuisine in descending order.

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 0, 
            name : 1, 
            cuisine : 1,
            grades : { grade : 1}, 
            borough : 1
        }
    }, 
    {
        $match : {
            cuisine : { $ne : 'American' },
            grades : { grade : 'A'}, 
            borough : { $ne : 'Brooklyn' }
        }
    }, 
    {
        $sort : {
            cuisine : -1
        }
    }
]);

// 14. Write a MongoDB query to find the restaurant Id, name, borough and cuisine for those restaurants which contain 'Wil' as first three letters for its name.

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 1, 
            name : 1, 
            borough : 1, 
            cuisine : 1,
        }
    }, 
    {
        $match : {
            name : { $regex : /^Wil/ }
        }
    }
]);

// 15. Write a MongoDB query to find the restaurant Id, name, borough and cuisine for those restaurants which contain 'ces' as last three letters for its name.

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 1, 
            name : 1, 
            borough : 1, 
            cuisine : 1,
        }
    }, 
    {
        $match : {
            name : { $regex : /ces$/}
        }
    }
]);

// 16. Write a MongoDB query to find the restaurant Id, name, borough and cuisine for those restaurants which contain 'Reg' as three letters somewhere in its name.

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 1, 
            name : 1, 
            borough : 1, 
            cuisine : 1,
        }
    }, 
    {
        $match : {
            name : { $regex : /Reg/}
        }
    }
]);

// 17. Write a MongoDB query to find the restaurants which belong to the borough Bronx and prepared either American or Chinese dish.

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 0, 
            name : 1,
            borough : 1, 
            cuisine : 1
        }
    },
    {
        $match : {
            borough : 'Bronx',
            cuisine : { $in : ['American', 'Chinese'] },
        }
    }
]);

// 18. Write a MongoDB query to find the restaurant Id, name, borough and cuisine for those restaurants which belong to the borough Staten Island or Queens or Bronxor Brooklyn.

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 1, 
            name : 1, 
            borough : 1,
            cuisine : 1
        }
    },
    {
        $match : {
            borough : { $in : ['Staten Island', 'Queens', 'Bronx', 'Brooklyn']}
        }
    }
]);

// 19. Write a MongoDB query to find the restaurant Id, name, borough and cuisine for those restaurants which are not belonging to the borough Staten Island or Queens or Bronxor Brooklyn.

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 1, 
            name : 1, 
            borough : 1,
            cuisine : 1
        }
    },
    {
        $match : {
            borough : { $nin : ['Staten Island', 'Queens', 'Bronx', 'Brooklyn'] }
        }
    }
]);

// 20. Write a MongoDB query to find the restaurant Id, name, borough and cuisine for those restaurants which achieved a score which is not more than 10. 

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 0, 
            name : 1, 
            borough : 1,
            cuisine : 1,
            grades : { score : 1 },
        }
    },
    {
        $match: {
          'grades.score' : { $lt : 10 }  
        }
    }
]);

// 21. Write a MongoDB query to find the restaurant Id, name, borough and cuisine for those restaurants which prepared dish except 'American' and 'Chinese' or restaurant's name begins with letter 'Wil'.

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 1, 
            name : 1, 
            borough : 1,
            cuisine : 1
        }
    },
    {
        $match : {
            name : { $regex : /^Wil/},
            cuisine : { $nin : ['American', 'Chinese']}
        }
    }
]);

// 22. Write a MongoDB query to find the restaurant Id, name, and grades for those restaurants which achieved a grade of "A" and scored 11 on an ISODate "2014-08-11T00:00:00Z" among many of survey dates..

db.getCollection(db_name).aggregate([
    {
        $project : {
            _id : 1, 
            name : 1, 
            grades : { 
                grade : 1,
                score : 1,
                date : 1
            }
        }
    },
    {
        $match : {
            'grades.grade' : 'A',
            'grades.score' : 11,
            'grades.date' :  ISODate('2014-08-11T00:00:00Z') 
        }
    }
]);

// 23. Write a MongoDB query to find the restaurant Id, name and grades for those restaurants where the 2nd element of grades array contains a grade of "A" and score 9 on an ISODate "2014-08-11T00:00:00Z".

// 24. Write a MongoDB query to find the restaurant Id, name, address and geographical location for those restaurants where 2nd element of coord array contains a value which is more than 42 and up to 52.. 

// 25. Write a MongoDB query to arrange the name of the restaurants in ascending order along with all the columns.

// 26. Write a MongoDB query to arrange the name of the restaurants in descending along with all the columns.

// 27. Write a MongoDB query to arranged the name of the cuisine in ascending order and for that same cuisine borough should be in descending order.

// 28. Write a MongoDB query to know whether all the addresses contains the street or not.

// 29. Write a MongoDB query which will select all documents in the restaurants collection where the coord field value is Double.

// 30. Write a MongoDB query which will select the restaurant Id, name and grades for those restaurants which returns 0 as a remainder after dividing the score by 7.

// 31. Write a MongoDB query to find the restaurant name, borough, longitude and attitude and cuisine for those restaurants which contains 'mon' as three letters somewhere in its name.

// 32. Write a MongoDB query to find the restaurant name, borough, longitude and latitude and cuisine for those restaurants which contain 'Mad' as first three letters of its name
