from urllib.error import HTTPError
from django.http import HttpResponse
import psycopg2
import json
import os

def health(request):
    try:
        conn = psycopg2.connect(dbname="dota2", user=os.environ.get('dbname'), host='147.175.150.216',password=os.environ.get('dbpassword'))
    except:
        print("I am unable to connect to the database")

    cur = conn.cursor()

    cur.execute("SELECT  version()")
    version = cur.fetchone()
    cur.execute("SELECT pg_database_size('dota2')/1024/1024 as dota2_db_size;")
    size = cur.fetchone()

    output = {
        "pgsql":{
            "version": str(version[0]),
            "dota2_db_size": size[0]
        }
    }
    output = json.dumps(output)
    
    return HttpResponse(output,content_type='application/json')

def patches(request):
    try:
        conn = psycopg2.connect(dbname="dota2", user=os.environ.get('dbname'), host='147.175.150.216',password=os.environ.get('dbpassword'))
    except:
        print("I am unable to connect to the database")
        
    cur = conn.cursor()
    output = list()

    cur.execute("""
        WITH 
        patches_dates AS(
            SELECT name as patch_version,
            CAST(EXTRACT(epoch from release_date) AS INTEGER) as patch_start_date,
            LEAD(CAST(EXTRACT(epoch from release_date) AS INTEGER),1) OVER (ORDER BY patches.id) as patch_end_date            FROM patches
        )

        SELECT patch_version, patch_start_date, patch_end_date,id as match_id, ROUND(matches.duration/60.0,2) as match_duration
        FROM patches_dates 
        LEFT JOIN matches ON matches.start_time BETWEEN patches_dates.patch_start_date AND patches_dates.patch_end_date
        ORDER BY patch_version
        
    """)
    patchNumber = '0'
    patches = None
    PatchDetail = None
    while(cur.rownumber < cur.rowcount):
        output = cur.fetchone()
        if(patchNumber != output[0]):
            if(PatchDetail != None):
                if(matchList):
                    PatchDetail["matches"] = matchList
                patches.append(PatchDetail)
            PatchDetail = {
                "patch_version" : output[0],
                "patch_start_date" : output[1],
                "patch_end_date" : output[2],
                "matches" : list()
            }
            if (patches == None):
                patches = list()

            matchList = list()
            patchNumber = output[0]
        if (output[3] != None):
            match = {
                "match_id" : output[3],
                "duration" : float(output[4])
            }
            matchList.append(match)
    if(PatchDetail != None):
        if(matchList):
            PatchDetail["matches"] = matchList
        patches.append(PatchDetail)
    output = {
        "patches" : patches
    }
    output = json.dumps(output)

    return HttpResponse(output,content_type='application/json')

def game_exp(request, player_id):
    try:
        conn = psycopg2.connect(dbname="dota2", user=os.environ.get('dbname'), host='147.175.150.216',password=os.environ.get('dbpassword'))
    except:
        print("I am unable to connect to the database")
        
    cur = conn.cursor()
    output = list()

    cur.execute("""
        SELECT players.id,nick as player_nick, heroes.localized_name as hero_localized_name, ROUND(matches.duration/60.0,2) as match_duration_minutes, 
        COALESCE(matches_players_details.xp_hero,0) + COALESCE(matches_players_details.xp_creep,0) + COALESCE(matches_players_details.xp_other,0) + COALESCE(matches_players_details.xp_roshan,0) AS experience_gained,
        matches_players_details.level as level_gained,
        CASE
            WHEN (matches_players_details.player_slot BETWEEN 0 AND 4) AND matches.radiant_win = 'true' THEN 'true' 
            WHEN (matches_players_details.player_slot BETWEEN 0 AND 4) AND matches.radiant_win = 'false' THEN 'false'	
            WHEN (matches_players_details.player_slot BETWEEN 128 AND 132) AND matches.radiant_win = 'true' THEN 'false' 
            WHEN (matches_players_details.player_slot BETWEEN 128 AND 132) AND matches.radiant_win = 'false' THEN 'true'
        END as winner,
        matches.id as match_id
        FROM players
        JOIN matches_players_details on players.id = matches_players_details.player_id
        JOIN heroes ON matches_players_details.hero_id = heroes.id
        JOIN matches ON matches_players_details.match_id = matches.id
        WHERE players.id = """ + str(player_id)+ "ORDER BY match_id")

    player_nick = None
    matches = list()
    while(cur.rownumber < cur.rowcount):
        output = cur.fetchone()
        if(cur.rownumber == 1):
            player_nick = output[1]
        if(output[6] == 'true'):
            winner = True
        else:
            winner = False
        match = {
            "match_id" : output[7],
            "hero_localized_name" : output[2],
            "match_duration_minutes" : float(output[3]),
            "experiences_gained" : output[4],
            "level_gained" : output[5],
            "winner" : winner
        }
        matches.append(match)


    output = {
        "id" : int(player_id),
        "player_nick" : player_nick,
        "matches" : matches
    }
    output = json.dumps(output)

    return HttpResponse(output,content_type='application/json')

def game_objectives(request, player_id):
    try:
        conn = psycopg2.connect(dbname="dota2", user=os.environ.get('dbname'), host='147.175.150.216',password=os.environ.get('dbpassword'))
    except:
        print("I am unable to connect to the database")
        
    cur = conn.cursor()
    output = list()

    cur.execute("""
            SELECT players.id,nick as player_nick, heroes.localized_name as hero_localized_name, matches.id as match_id,
            COALESCE(game_objectives.subtype,'NO_ACTION') as hero_action, count(COALESCE(game_objectives.subtype,'NO_ACTION'))
            FROM players
            JOIN matches_players_details on players.id = matches_players_details.player_id
            JOIN heroes ON matches_players_details.hero_id = heroes.id
            JOIN matches ON matches_players_details.match_id = matches.id
            LEFT JOIN game_objectives ON game_objectives.match_player_detail_id_1 = matches_players_details.id
            WHERE players.id = """ + str(player_id) + """
            GROUP BY players.id, nick,heroes.localized_name,matches.duration,matches.id,game_objectives.subtype
            ORDER BY match_id
        """)

    player_nick = None
    matches = None
    matchID = None
    PlayerActionDetail = None
    actionCount = 0
    while(cur.rownumber < cur.rowcount):
        output = cur.fetchone()
        if(cur.rownumber == 1):
            player_nick = output[1]

        if(matchID != output[3]):
            if(PlayerActionDetail != None):
                if(actions):
                    PlayerActionDetail["actions"] = actions
                matches.append(PlayerActionDetail)
            PlayerActionDetail = {
                "match_id" : output[3],
                "hero_localized_name" : output[2],
                "actions" : list(),
            }
            if(matches == None):
                matches = list()
            actions = list()
            matchID = output[3]
        action = {
            "hero_action" : output[4],
            "count" : output[5]}
        actions.append(action)
    if(PlayerActionDetail != None):
        if(action):
            PlayerActionDetail["actions"] = actions
        matches.append(PlayerActionDetail)
    output = {
        "id" : int(player_id),
        "player_nick" : player_nick,
        "matches" : matches
    }
    output = json.dumps(output)

    return HttpResponse(output,content_type='application/json')

def abilities(request, player_id):
    try:
        conn = psycopg2.connect(dbname="dota2", user=os.environ.get('dbname'), host='147.175.150.216',password=os.environ.get('dbpassword'))
    except:
        print("I am unable to connect to the database")
        
    cur = conn.cursor()
    output = list()

    cur.execute("""
        SELECT players.id,COALESCE(nick,'unknown') as player_nick, heroes.localized_name as hero_localized_name, matches.id as match_id
        ,abilities.name as ability_name,count(abilities.name), max(ability_upgrades.level) as upgrade_level

        FROM players
        JOIN matches_players_details on players.id = matches_players_details.player_id
        JOIN heroes ON matches_players_details.hero_id = heroes.id
        JOIN matches ON matches_players_details.match_id = matches.id
        JOIN ability_upgrades ON ability_upgrades.match_player_detail_id = matches_players_details.id
        JOIN abilities ON ability_upgrades.ability_id = abilities.id
        WHERE players.id = """ + str(player_id) + """
        GROUP BY players.id,heroes.localized_name,matches.duration,matches.id,abilities.name
        ORDER BY match_id
        """)

    player_nick = None
    matches = None
    matchID = None
    PlayerAbilityDetail = None
    actionCount = 0
    while(cur.rownumber < cur.rowcount):
        output = cur.fetchone()
        if(cur.rownumber == 1):
            player_nick = output[1]

        if(matchID != output[3]):
            if(PlayerAbilityDetail != None):
                if(abilities):
                    PlayerAbilityDetail["abilities"] = abilities
                matches.append(PlayerAbilityDetail)
            PlayerAbilityDetail = {
                "match_id" : output[3],
                "hero_localized_name" : output[2],
                "abilities" : list()
            }
            if(matches == None):
                matches = list()
            abilities = list()
            matchID = output[3]
        ability = {
            "ability_name" : output[4],
            "count" : output[5],
            "upgrade_level" : output[6]
        }
        abilities.append(ability)

    if(PlayerAbilityDetail != None):
        if(ability):
            PlayerAbilityDetail["abilities"] = abilities
        matches.append(PlayerAbilityDetail)

    output = {
        "id" : int(player_id),
        "player_nick" : player_nick,
        "matches" : matches
    }
    output = json.dumps(output)

    return HttpResponse(output,content_type='application/json')