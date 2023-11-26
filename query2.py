import sqlite3
import time
import requests
from neo4j import GraphDatabase

con = sqlite3.connect("database.sqlite")
cur = con.cursor()

neo4j_url = "bolt://localhost:7687"  # Adjust the URL based on your Neo4j server configuration
neo4j_auth = ("neo4j", "datamanagement")  # Adjust the username and password

uri = "bolt://localhost:7687"
username = "neo4j"
password = "soccerDB"
driver = GraphDatabase.driver(uri, auth=(username, password))

def measure_Query(num, q_sql, q_neo4j):
    # Esecuzione della query in SQL
    print(f"Executing Query {num} in SQL:")
    begin_time_sql = time.time()
    print(cur.execute(q_sql).fetchall())
    
    end_time_sql = time.time()
    print(f"Done! Time elapsed: {(end_time_sql - begin_time_sql) * 1000} ms")

    # Esecuzione della query in Neo4j
    print(f"\nExecuting Query {num} in Neo4j:")
    begin_time_neo4j = time.time()
    with driver.session() as session:
        records = session.run(q_neo4j)
        print( [ dict(i) for i in records] )
        
    end_time_neo4j = time.time()
    print(f"Done! Time elapsed: {(end_time_neo4j - begin_time_neo4j) * 1000} ms")
    




# Define your Neo4j queries
q1_sql = """
SELECT team.team_long_name, count(*)
FROM country, match, team
WHERE country.id = match.country_id 
AND country.name = "Italy"
AND (match.home_team_api_id = team.team_api_id
OR match.away_team_api_id = team.team_api_id)
GROUP BY team.team_long_name	
"""
q1_neo4j = """
MATCH (t:Team)-[r]->(:Match)-[:PLAYED_IN]->(:Country {name:'Italy'})
WHERE type(r) = 'PLAYED_AS_AWAY' OR type(r) = 'PLAYED_AS_HOME'
RETURN t.team_long_name,count(t)
"""
q2_sql="""
SELECT team.team_long_name, count(*) as vinte
FROM match, team
WHERE (match.home_team_api_id = team.team_api_id AND match.home_team_goal > match.away_team_goal)
OR (match.away_team_api_id = team.team_api_id AND
match.away_team_goal > match.home_team_goal)
GROUP BY team.team_long_name
ORDER BY vinte desc
LIMIT 1;
"""
q2_neo4j="""
MATCH (s:Team)-[r]->(m:Match)
WHERE (type(r) = 'PLAYED_AS_HOME' AND m.home_team_goal>m.away_team_goal)
OR (type(r) = 'PLAYED_AS_AWAY' AND m.away_team_goal > m.home_team_goal)
RETURN s.team_long_name, count(s) as vinte
ORDER BY vinte DESC LIMIT 1;
"""

q3_sql="""
SELECT distinct count(*)
FROM match, league, team, country
WHERE country.name = "Spain" AND country.id = league.country_id AND league.id = match.league_id AND (match.home_team_api_id = team.team_api_id OR match.away_team_api_id = team.team_api_id)
GROUP BY country.name
"""
q3_neo4j="""
MATCH (t:Team)-->(:Match)-[:FOR_LEAGUE]->(:League)-[:BASED_IN]->(:Country {name:'Spain'}) 
RETURN count(t);
"""

q4_sql="""
SELECT DISTINCT t1.team_long_name, t2.team_long_name
FROM team as t1, team as t2, match as m1, match as m2
WHERE 
(m1.away_team_api_id = m2.home_team_api_id)
AND
t2.team_api_id = m2.home_team_api_id
AND
t1.team_api_id = m1.home_team_api_id
"""

q4_neo4j="""
MATCH (t1:Team)-[:PLAYED_AS_HOME]->(m1:Match)<-[:PLAYED_AS_AWAY]-(t2:Team)-[:PLAYED_AS_HOME]->(m2:Match)
RETURN DISTINCT t1.team_long_name,t2.team_long_name
"""


q5_sql="""
SELECT distinct t1.team_long_name ,t2.team_long_name , t3.team_long_name
FROM team as t1,team as t2, team as t3, match as m1, match as m2, match as m3
WHERE (m1.away_team_api_id = m2.home_team_api_id AND m1.home_team_goal > m1.away_team_goal)
AND
(m2.away_team_api_id = m3.home_team_api_id AND m2.home_team_goal > m2.away_team_goal AND m2.season = m1.season)
AND
(m3.home_team_goal > m3.away_team_goal AND m3.season = m2.season)
AND
t1.team_api_id = m1.home_team_api_id
AND
t2.team_api_id = m2.home_team_api_id
AND
t3.team_api_id = m3.home_team_api_id
"""

q5_neo4j="""
MATCH (t1:Team)-[:PLAYED_AS_HOME]->(m1:Match)<-[:PLAYED_AS_AWAY]-(t2:Team)-[:PLAYED_AS_HOME]->(m2:Match)<-[:PLAYED_AS_AWAY]-(t3:Team)-[:PLAYED_AS_HOME]->(m3:Match)
WHERE m1.home_team_goal > m1.away_team_goal AND m2.home_team_goal>m2.away_team_goal AND m3.home_team_goal>m3.away_team_goal
AND m1.season = m2.season AND m2.season = m3.season
RETURN DISTINCT t1.team_long_name,t2.team_long_name,t3.team_long_name;
"""

q6_sql = """
ALTER TABLE match
DROP COLUMN B365H
"""

q6_neo4j = """
MATCH (m:Match) REMOVE m.B365H
"""


q7_sql = """
ALTER TABLE match
ADD total_goals5 SMALLINT;
UPDATE match
SET total_goals5 = home_team_goal + away_team_goal;
"""

q7_neo4j = """
MATCH (m:Match) SET m.total_goals5 = m.away_team_goal + m.home_team_goal; 
"""

q8_sql = """
SELECT p.player_name, AVG(pa.overall_rating) AS avg_rating
FROM Player_Attributes pa
JOIN Player p ON pa.player_api_id = p.player_api_id
GROUP BY p.player_api_id
ORDER BY avg_rating DESC
LIMIT 5;


"""

q8_neo4j = """
MATCH (p:Player)-[:HAS_ATTRIBUTES]->(pa:Player_Attributes)
RETURN p.player_name AS player, AVG(pa.overall_rating) AS avg_rating
ORDER BY avg_rating DESC
LIMIT 5;




"""
# Continue defining other queries (q2_sql, q2_neo4j, q3_sql, q3_neo4j, etc.)

# Execute queries

#input("First query: Return number of matches played by every team in Italy.\nPress enter to start...")
#measure_Query("1", q1_sql, q1_neo4j)
# Repeat the measure_Query function for other queries
#input("\nSecond query: Return name of the team that won more matches.\nPress enter to start...")
#measure_Query("2",q2_sql,q2_neo4j)
#input("\nThird query: Return number of teams that have played at least one game in a Spanish League.\nPress enter to start...")
#measure_Query("3",q3_sql,q3_neo4j)
#input("\nFourth query: Return the chain of home-away team.\nPress enter to start...")
#measure_Query("4.1",q4_sql,q4_neo4j)
#input("\nFifth query: Return the chain of home-away teams in the same season where home always won.\nPress enter to start...")
#measure_Query("5",q5_sql,q5_neo4j)
input("\nEight query: Return league and country of all players that played matches where home team won with a goal difference of more than three goals.\nPress enter to start...")
measure_Query("8",q8_sql,q8_neo4j)

# Close connections
con.close()

