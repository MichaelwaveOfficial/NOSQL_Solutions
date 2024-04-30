// final build ONLY!

    // Clean, production ready queries, w/ comments. :)

     * script 

    * HI, MY NAME IS MICHAEL KING, 
    FROM THE GENERAL COMPUTING PATHWAY, 
    STUENT NUMBER 2141866,

    * THIS IS A SHORT VIDEO DEMONSTRATION, SHOWING MY SOLUTION TO THE DATABASE 3 ASSIGNMENT, 
        CONCERNED WITH MODELLING A SIMPLE GRAPH DB REPRESENTING THE ENGLISH PREMIER LEGAUGE. 

    * DESIGN AND JUSTIFICATION OF THE GRAPH MODEL
        * KEEP SIMPLE, ALLOW FOR REPETITITON OF VALUES:
            * KEEPS QUERIES SIMPLE. 
            * EASIER TO UNPACK INFORMATION. 
            * INTRODUCES STORAGE OVERHEADS BUT THATS NOT THE CONCERN SPEED IS. 
            * RELATIONSHIPS MODELLED AS HOME AND AWAY,
                * ALLOWS FOR EXTRA ATTRIBUTES TO BE ADDED. 
            * ORIGINAL WAS A LOT MORE COMPLEX DUE TO RELATIONAL DESIGN HABBITS, WAS A LOT SLOWER BECAUSE OF 
                COMPLEXITY. 

    * TALK THROUGH QUERIES, KEEP BRIEF.

    // Define Constraints -- 

        // Establish structure and integrity within the db. 

        // Ensure that team names are unqiue to mitigate redundancies. 
        CREATE CONSTRAINT unique_team_name IF NOT EXISTS
        FOR (team:TEAM) 
        REQUIRE team.Name IS UNIQUE;

        // Ensure that two teams cannot conflict. 
        CREATE CONSTRAINT unique_match_details IF NOT EXISTS
        FOR (game:GAME)
        REQUIRE (game.home_team, game.away_team) IS UNIQUE;

    // Implement Indexes --

        // Despite storage overhead, most commonly queried nodes recieve indexes to enhance query performance.
            // index represents defined values. 

        // NODE INDEXES // 
        
        // Teams
        CREATE INDEX team_index IF NOT EXISTS
        FOR (team:TEAM)
        on team.Name;

        // Games    
        CREATE INDEX game_index IF NOT EXISTS
        FOR (game:GAME)
        ON game.Round;

        // RELATIONSHIP INDEXES // 

        // Indexes for the relationships results since those will be queried frequently. 
        CREATE INDEX relationship_win_index IF NOT EXISTS 
        FOR ()-[relationship:WINS]-()
        ON( relationship.HT_Wins, relationship.FT_Wins);

        CREATE INDEX relationship_draw_index IF NOT EXISTS
        FOR ()-[relationship:DRAWS]-()
        ON (relationship.HT_Wins, relationship.FT_Wins);

        CREATE INDEX relationship_loss_index IF NOT EXISTS
        FOR ()-[relationship:LOSES]-()
        ON (relationship.HT_Wins, relationship.FT_Wins);

    // Data Importation //

        // Part One

        // Grab headers from csv, assign those values to nodes.
        LOAD CSV WITH HEADERS FROM 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR6s_XHFq6Rmo9ngx9UVpkPjCmAChHh9mDIeKwS8hzJT2TPxIzM5ZUk4FwmZQf-uQy5wBIdr15UXjCC/pub?output=csv' AS file
        // Merge will only create if no patterns present.
        MERGE (home_team:TEAM {Name:file.Team1})
        MERGE (away_team:TEAM {Name:file.Team2})
        MERGE (game:GAME {
            Date:file.Date,
            Round:toInteger(file.Round),
            HomeTeam:file.Team1,
            AwayTeam:file.Team2,
            HalfTime:file.HT,
            FullTime:file.FT
        })
        // Assert when team playing home or away, modelled through relationships.
        CREATE (game)-[:HOME_TEAM]->(home_team)
        CREATE (game)-[:AWAY_TEAM]->(away_team)
        // split score results and store in integer variables, will be stored in relationships.
        WITH 
            toInteger(split(file.HT, '-')[0]) AS HTHomeScore,
            toInteger(split(file.HT, '-')[1]) AS HTAwayScore, 

            toInteger(split(file.FT, '-')[0]) AS FTHomeScore,
            toInteger(split(file.FT, '-')[1]) AS FTAwayScore,
            
        game AS game, home_team AS home_team, away_team AS away_team 
        // relationships win/draw/loss status will be determined by comparing computed scores from above. 
        // for a draw case where scores from both teams equal one another. 
        FOREACH (
            file IN CASE WHEN FTHomeScore = FTAwayScore THEN [1] ELSE [] END |
                MERGE (home_team)-[home_draw:DRAWS{
                    HT_Wins: HTHomeScore,
                    FT_Wins: FTHomeScore
                }]->(game)<-[away_draw:DRAWS{
                    HT_Wins: HTAwayScore,
                    FT_Wins: FTAwayScore
                }]-(away_team)
        )
        // win case for home team where there score is greater. 
        FOREACH (
            file IN CASE WHEN FTHomeScore > FTAwayScore THEN [1] ELSE [] END |
                MERGE (home_team)-[home_win:WINS{
                    HT_Wins: HTHomeScore,
                    FT_Wins: FTHomeScore
                }]->(game)<-[away_loss:LOSSES{
                    HT_Wins: HTAwayScore,
                    FT_Wins: FTAwayScore
                }]-(away_team)
        )
        // loss case for home team where away score is greater. 
        FOREACH (
            file IN CASE WHEN FTHomeScore < FTAwayScore THEN [1] ELSE [] END |
                MERGE (home_team)-[home_loss:LOSSES{
                    HT_Wins: HTHomeScore,
                    FT_Wins: FTHomeScore
                }]->(game)<-[away_win:WINS{
                    HT_Wins: HTAwayScore,
                    FT_Wins: FTAwayScore
                }]-(away_team)
        )

        // Part Two, demonstrate month conversion for query sake. 

            LOAD CSV WITH HEADERS FROM 'https://docs.google.com/spreadsheets/d/e/2PACX-1vR6s_XHFq6Rmo9ngx9UVpkPjCmAChHh9mDIeKwS8hzJT2TPxIzM5ZUk4FwmZQf-uQy5wBIdr15UXjCC/pub?output=csv' AS file
            WITH SPLIT(file.Date, ' ') AS game_date, file
            MERGE (home_team:TEAM {Name:file.Team1})
            MERGE (away_team:TEAM {Name:file.Team2})
            MERGE (game:GAME {
                Date: file.Date, 
                Day: toInteger(game_date[1]),
                Month: CASE game_date[2]
                    WHEN 'Jan' THEN 1
                    WHEN 'Feb' THEN 2
                    WHEN 'Mar' THEN 3
                    WHEN 'Apr' THEN 4
                    WHEN 'May' THEN 5
                    WHEN 'Jun' THEN 6
                    WHEN 'Jul' THEN 7
                    WHEN 'Aug' THEN 8
                    WHEN 'Sep' THEN 9
                    WHEN 'Oct' THEN 10
                    WHEN 'Nov' THEN 11
                    WHEN 'Dec' THEN 12
                END,
                Year: toInteger(game_date[3]),
                Week: toInteger(SUBSTRING(game_date[4], 1, SIZE(game_date[4]) -2)),
                Round:toInteger(file.Round),
                HomeTeam:file.Team1,
                AwayTeam:file.Team2,
                HalfTime:file.HT,
                FullTime:file.FT
            })
            CREATE (game)-[:HOME_TEAM]->(home_team)
            CREATE (game)-[:AWAY_TEAM]->(away_team)
            WITH 
                toInteger(split(file.HT, '-')[0]) AS HTHomeScore,
                toInteger(split(file.HT, '-')[1]) AS HTAwayScore, 

                toInteger(split(file.FT, '-')[0]) AS FTHomeScore,
                toInteger(split(file.FT, '-')[1]) AS FTAwayScore,
                
            game AS game, home_team AS home_team, away_team AS away_team 
            FOREACH (
                file IN CASE WHEN FTHomeScore = FTAwayScore THEN [1] ELSE [] END |
                    MERGE (home_team)-[home_draw:DRAWS{
                        HT_Wins: HTHomeScore,
                        FT_Wins: FTHomeScore
                    }]->(game)<-[away_draw:DRAWS{
                        HT_Wins: HTAwayScore,
                        FT_Wins: FTAwayScore
                    }]-(away_team)
            )
            FOREACH (
                file IN CASE WHEN FTHomeScore > FTAwayScore THEN [1] ELSE [] END |
                    MERGE (home_team)-[home_win:WINS{
                        HT_Wins: HTHomeScore,
                        FT_Wins: FTHomeScore
                    }]->(game)<-[away_loss:LOSSES{
                        HT_Wins: HTAwayScore,
                        FT_Wins: FTAwayScore
                    }]-(away_team)
            )
            FOREACH (
                file IN CASE WHEN FTHomeScore < FTAwayScore THEN [1] ELSE [] END |
                    MERGE (home_team)-[home_loss:LOSSES{
                        HT_Wins: HTHomeScore,
                        FT_Wins: FTHomeScore
                    }]->(game)<-[away_win:WINS{
                        HT_Wins: HTAwayScore,
                        FT_Wins: FTAwayScore
                    }]-(away_team)
            )
    
        MATCH (n) RETURN n

    // Constraint Enforcement // 

        // Confirm indexes and constraints have been implemented successfully. 
        :schema
    
        // Check constraints are being enforced by introducing erroneous data.
        
        // Violates unique team constraint. 
        CREATE (team:TEAM {name: 'Arsenal FC'});
        
        // Attestation
        MATCH (team:TEAM {name : 'Arsenal FC'})
        RETURN COUNT(team);

        // Violates unique match details constraint. 
        CREATE(game:GAME {HomeTeam: 'Arsenal FC', AwayTeam: 'Arsenal FC'});
        
        // Attestation 
        MATCH (game:GAME {HomeTeam: 'Arsenal FC', AwayTeam: 'Arsenal FC'})
        RETURN COUNT(game);

    // Queries

        // Provided assignment queries to understand the data provided. 

        // 1. Display the total number of matches played. // 
            
            // Fetch games nodes, count number found, return as matches played.
            MATCH (games:GAME)
            RETURN COUNT(games) AS matches_played

        // 2. Display details of all matches involving ‘Manchester United FC’. // 

            // Fetch game nodes where associated home and away teams contain the name 'Manchester United FC'.
            MATCH (games:GAME)-[:HOME_TEAM | :AWAY_TEAM]->(teams:TEAM {Name: 'Manchester United FC'}) 
            RETURN games, teams

        // 3. Display all the teams that played the EPL matches in the season. // 

            // All teams in file are within the epl? 

            // Match all teams with a newly created league node represented as the epl.
            MATCH (team:TEAM)
            MERGE (epl:LEAGUE {Name: 'English Premier League'})
            WITH team, epl 
            // establish and model a relationship representing the teams that have played in the epl.
            CREATE (team)-[:PLAYED_IN]->(epl)
            RETURN team, epl

        // 4. Display the team with the most ‘wins’ in January. // 

            // Source all teams with WINS modelled into their relationships
            MATCH (team:TEAM)-[relationship:WINS]->(game:GAME)
            // Find the wins where the match date contains 'January'.
            WHERE game.Date CONTAINS 'Jan' OR game.Month = 1
            // Count the number of wins found in the associations. 
            WITH team, game, COUNT(relationship) AS wins
            RETURN team, game
            // Order from most to least, limit to the 1 as top result. 
            ORDER BY wins DESC 
            LIMIT 1

        // 5. Display the top five teams that have the best scoring power. //

            // Find the teams associated with games. 
            MATCH(team:TEAM)-[relationship]->(game:GAME)
            // Return the team name, sum together their fulltime wins to model goals scored.
            RETURN team.Name AS team, SUM(relationship.FT_Wins) AS scoring_power 
            // Order from most to least, limit 5. 
            ORDER BY scoring_power DESC 
            LIMIT 5

        // 6. Display the top five teams that have the worst defending. //

            // Match away team as well to get their fulltime wins. 
            MATCH(home_team:TEAM)-[home_relationship]->(game:GAME)<-[away_relationship]-(away_team:TEAM)
            // return team and sum together the away teams scores against as worst defending. 
            RETURN home_team.Name AS team, SUM(away_relationship.FT_Wins) AS worst_defending
            ORDER BY worst_defending DESC
            LIMIT 5 

        // 7. Display top five teams that have the best winning record // 

            // Match teams with win relationships. 
            MATCH (team:TEAM)-[relationship:WINS]->(game:GAME)
            // count these relationships as winning_record.
            WITH team, COUNT(relationship) AS winning_record
            // return teams with wins, sort from most to least, limit to 5. 
            RETURN team.Name as team_name, winning_record
            ORDER BY winning_record DESC 
            LIMIT 5
    

        // 8. Display top five teams with the best half time result. // 

            // Grab teams and their associations with games. 
            MATCH (team:TEAM)-[relationship]->(game:GAME)
            // Return the teams with highest halftime result modelled in the relationship, sorted from most to least, limited to 5. 
            RETURN team.Name AS team_name, MAX(relationship.HT_Wins) AS best_half_time_result
            ORDER BY best_half_time_result DESC 
            LIMIT 5

        // 9. Display the team with the most consecutive ‘wins’. // 

            // Fetch teams with wins.
            MATCH (team:TEAM)-[relationship:WINS]->(game:GAME)
            WITH 
                // get relationship, game dates and team stored. 
                type(relationship) AS game_result,
                COLLECT([game.Day, game.Month]) AS game_dates,
                team.Name AS team_name
            // Convert game dates into a list. 
            UNWIND game_dates as listed_dates 
            WITH game_result, listed_dates, team_name
            // Put dates into order. 
            ORDER BY listed_dates
            RETURN team_name, 
            // Iterate over new attributes to store current and highest win streaks. 
            REDUCE(streak = {
                current_streak: 0,
                highest_streak: 0
            },
            // Find each record with a win.
            record IN COLLECT(game_result) |

                CASE WHEN record = 'WINS'
                    // Add to current streak if it beats the highest streak. Otherwise, end the streak and store. 
                    THEN {
                        current_streak: streak.current_streak + 1,
                        highest_streak: CASE WHEN streak.highest_streak < streak.current_streak + 1
                        THEN streak.current_streak + 1 
                        ELSE streak.highest_streak END
                    } ELSE {
                        highest_streak: streak.highest_streak,
                        current_streak: 0
                    }
                END
            ).highest_streak AS consecutive_wins
            // order from highest to lowest, limit one to source the highest streaker. 
            ORDER BY consecutive_wins DESC
            LIMIT 1

        // End of demo, drop datbase.

            // Drop all nodes and their relationships.
            MATCH (n) DETACH DELETE n;

            // Drop constraints.
            DROP CONSTRAINT unique_team_name IF EXISTS;
            DROP CONSTRAINT unique_match_details IF EXISTS;

            // Drop indexes.
            DROP INDEX team_index IF EXISTS;
            DROP INDEX game_index IF EXISTS;
            DROP INDEX relationship_win_index IF EXISTS;
            DROP INDEX relationship_loss_index IF EXISTS;
            DROP INDEX relationship_draw_index IF EXISTS;

// End of file, also stops file from breaking?