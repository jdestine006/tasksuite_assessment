# candidate_solution.py
import sqlite3
import os
from fastapi import FastAPI, HTTPException
from typing import List, Optional
import requests
import uvicorn

# --- Constants ---
DB_NAME = "pokemon_assessment.db"


# --- Database Connection ---
def connect_db() -> Optional[sqlite3.Connection]:
    """
    Task 1: Connect to the SQLite database.
    Implement the connection logic and return the connection object.
    Return None if connection fails.
    """
    if not os.path.exists(DB_NAME):
        print(f"Error: Database file '{DB_NAME}' not found.")
        return None

    conn = None
    try:
        # --- Implement Here ---
        conn = sqlite3.connect(DB_NAME)
        # --- End Implementation ---
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        return None

    return conn

# --- Data Cleaning ---
def clean_database(conn: sqlite3.Connection):
    """
    Task 2: Clean up the database using the provided connection object.
    Implement logic to:
    - Remove duplicate entries in tables (pokemon, types, abilities, trainers).
      Choose a consistent strategy (e.g., keep the first encountered/lowest ID).
    - Correct known misspellings (e.g., 'Pikuchu' -> 'Pikachu', 'gras' -> 'Grass', etc.).
    - Standardize casing (e.g., 'fire' -> 'Fire' or all lowercase for names/types/abilities).
    """
    if not conn:
        print("Error: Invalid database connection provided for cleaning.")
        return

    cursor = conn.cursor()
    print("Starting database cleaning...")

    try:
        # --- Implement Here ---
        cursor.execute('''
        DELETE FROM abilities 
        WHERE id NOT IN (
            SELECT MIN(id) 
            FROM abilities
            GROUP BY name);
''')
        cursor.execute('''
        DELETE FROM pokemon 
        WHERE id NOT IN (
            SELECT MIN(id) 
            FROM pokemon
            GROUP BY name, type1_id, type2_id);
''')
        cursor.execute('''
        DELETE FROM trainer_pokemon_abilities 
        WHERE id NOT IN (
            SELECT MIN(id) 
            FROM trainer_pokemon_abilities
            GROUP BY trainer_id, pokemon_id, ability_id);
''')
        cursor.execute('''
        DELETE FROM trainers
        WHERE id NOT IN (
            SELECT MIN(id) 
            FROM trainers
            GROUP BY name);
''')
        cursor.execute('''
        DELETE FROM types 
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM types
            GROUP BY name);
''')
        cursor.execute('''
        DELETE FROM abilities WHERE name = 'Remove this ability';
        ''')
        # Standardize all type names to Title Case (e.g., 'water' -> 'Water')
        cursor.execute('''
        UPDATE types SET name =
            CASE
                WHEN name IS NOT NULL THEN substr(UPPER(name), 1, 1) || LOWER(substr(name, 2))
                ELSE name
            END;
        ''')
        cursor.execute('''
        UPDATE abilities SET name = CASE
            WHEN name = 'static' THEN 'Static'
            WHEN name = 'gras' THEN 'Grass'
            WHEN name = 'overgrow' THEN 'Overgrow'
            WHEN name = 'Torrent' THEN 'Torrent'
            ELSE name
        END;
        ''')
        cursor.execute('''
        UPDATE pokemon SET type2_id = (
            SELECT t.id FROM types t WHERE t.name = (
                substr(UPPER((SELECT name FROM types WHERE id = type2_id)), 1, 1) || LOWER(substr((SELECT name FROM types WHERE id = type2_id), 2))
            )
        ) WHERE type2_id IS NOT NULL;
        ''')
        cursor.execute('''
        UPDATE trainers SET name = CASE
            WHEN name = 'misty' THEN 'Misty'          
            ELSE name             
        END;
        ''')
        cursor.execute('''
        DELETE FROM trainers WHERE name = '' OR name IS NULL
        ''')
        cursor.execute('''
        UPDATE pokemon SET name = CASE
            WHEN name = 'Pikuchu' THEN 'Pikachu'
            WHEN name = 'Charmanderr' THEN 'Charmander'
            WHEN name = 'Bulbasuar' THEN 'Bulbasaur'
            WHEN name = 'RATtata' THEN 'Rattata'
            ELSE name
        END;
        ''')
        # Standardize all ability names to Title Case for each hyphen-separated part
        cursor.execute('SELECT id, name FROM abilities WHERE name IS NOT NULL;')
        abilities = cursor.fetchall()
        for ability_id, ability_name in abilities:
            # Title-case each hyphen-separated part
            new_name = '-'.join(part.capitalize() for part in ability_name.split('-'))
            if new_name != ability_name:
                cursor.execute('UPDATE abilities SET name = ? WHERE id = ?;', (new_name, ability_id))
        # --- End Implementation ---
        conn.commit()
        print("Database cleaning finished and changes committed.")

    except sqlite3.Error as e:
        print(f"An error occurred during database cleaning: {e}")
        conn.rollback()

# --- FastAPI Application ---
def create_fastapi_app() -> FastAPI:
    """
    FastAPI application instance.
    Define the FastAPI app and include all the required endpoints below.
    """
    print("Creating FastAPI app and defining endpoints...")
    app = FastAPI(title="Pokemon Assessment API")

    # --- Define Endpoints Here ---
    @app.get("/")
    def read_root():
        """
        Task 3: Basic root response message
        Return a simple JSON response object that contains a `message` key with any corresponding value.
        """
        # --- Implement here ---
        return {"message": "FastAPI endpoint reached."}
        # --- End Implementation ---

    @app.get("/pokemon/ability/{ability_name}", response_model=List[str])
    def get_pokemon_by_ability(ability_name: str):
        """
        Task 4: Retrieve all Pokémon names with a specific ability.
        Query the cleaned database. Handle cases where the ability doesn't exist.
        """
        # --- Implement here ---
        conn = connect_db()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection error.")
        with conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    SELECT p.name FROM pokemon p
                    JOIN trainer_pokemon_abilities tpa ON p.id = tpa.pokemon_id
                    JOIN abilities a ON tpa.ability_id = a.id
                    WHERE a.name = ? COLLATE NOCASE;
                ''', (ability_name,))
                results = cursor.fetchall()
                return [row[0] for row in results] if results else []

        # --- End Implementation ---

    @app.get("/pokemon/type/{type_name}", response_model=List[str])
    def get_pokemon_by_type(type_name: str):
        """
        Task 5: Retrieve all Pokémon names of a specific type (considers type1 and type2).
        Query the cleaned database. Handle cases where the type doesn't exist.
        """
        # --- Implement here ---
        conn = connect_db()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection error.")
        with conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    SELECT p.name FROM pokemon p
                    JOIN types t1 ON p.type1_id = t1.id
                    LEFT JOIN types t2 ON p.type2_id = t2.id
                    WHERE t1.name = ? COLLATE NOCASE OR t2.name = ? COLLATE NOCASE;
                ''', (type_name, type_name))
                results = cursor.fetchall()
                return [row[0] for row in results] if results else []
        # --- End Implementation ---

    @app.get("/trainers/pokemon/{pokemon_name}", response_model=List[str])
    def get_trainers_by_pokemon(pokemon_name: str):
        """
        Task 7: Retrieve all trainer names who have a specific Pokémon.
        Query the cleaned database. Handle cases where the Pokémon doesn't exist or has no trainer.
        """
        # --- Implement here ---
        conn = connect_db()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection error.")
        with conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    SELECT DISTINCT t.name FROM trainers t
                    JOIN trainer_pokemon_abilities tpa ON t.id = tpa.trainer_id
                    JOIN pokemon p ON tpa.pokemon_id = p.id
                    WHERE p.name = ?;
                ''', (pokemon_name,))
                results = cursor.fetchall()
                return [row[0] for row in results] if results else []
        # --- End Implementation ---

    @app.get("/abilities/pokemon/{pokemon_name}", response_model=List[str])
    def get_abilities_by_pokemon(pokemon_name: str):
        """
        Task 8: Retrieve all ability names of a specific Pokémon.
        Query the cleaned database. Handle cases where the Pokémon doesn't exist.
        """
        # --- Implement here ---
        conn = connect_db()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection error.")
        with conn:
            with conn.cursor() as cursor:
                cursor.execute('''
                    SELECT ab.name FROM abilities ab
                    JOIN trainer_pokemon_abilities tpa ON ab.id = tpa.ability_id
                    JOIN pokemon p ON tpa.pokemon_id = p.id
                    WHERE p.name = ? COLLATE NOCASE;
                ''', (pokemon_name,))
                results = cursor.fetchall()
                return [row[0] for row in results] if results else []

    @app.post("/trainer-pokemon-abilities/{pokemon_name}")
    def add_trainer_pokemon_abilities(pokemon_name: str):
        # Check if the pokemon exists in PokeAPI before inserting
        conn = connect_db()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection error.")
        try:
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute('''SELECT id FROM pokemon WHERE name = ?''', (pokemon_name,))
                    existing_pokemon = cursor.fetchone()
                    if existing_pokemon:
                        raise HTTPException(status_code=400, detail="Pokémon already exists in the database.")

                    response = requests.get(f"https://pokeapi.co/api/v2/pokemon/{pokemon_name}")
                    if response.status_code != 200:
                        raise HTTPException(status_code=404, detail="Pokémon not found in PokéAPI.")
                    data = response.json()

                    abilities = [a["ability"]["name"] for a in data["abilities"]]
                    type1 = data["types"][0]["type"]["name"] if len(data["types"]) > 0 else None
                    type2 = data["types"][1]["type"]["name"] if len(data["types"]) > 1 else None

                    # Handle Pokemon by Types
                    def get_or_create_type(type_name):
                        if not type_name:
                            return None
                        cursor.execute('''SELECT id FROM types WHERE name = ?''', (type_name,))
                        row = cursor.fetchone()
                        if row:
                            return row[0]
                        cursor.execute('''INSERT INTO types (name) VALUES (?)''', (type_name,))
                        return cursor.lastrowid

                    type1_id = get_or_create_type(type1)
                    type2_id = get_or_create_type(type2)

                    # Handle Pokémon by Name
                    cursor.execute('''SELECT id FROM pokemon WHERE name = ?''', (pokemon_name,))
                    row = cursor.fetchone()
                    if row:
                        pokemon_id = row[0]
                    else:
                        cursor.execute('''INSERT INTO pokemon (name, type1_id, type2_id) VALUES (?, ?, ?)''', (pokemon_name, type1_id, type2_id))
                        pokemon_id = cursor.lastrowid

                    # Assigning a random trainer to Pokemon if none are found
                    cursor.execute('''SELECT id FROM trainers ORDER BY RANDOM() LIMIT 1''')
                    trainer_row = cursor.fetchone()
                    if not trainer_row:
                        raise HTTPException(status_code=400, detail="No trainers available.")
                    trainer_id = trainer_row[0]

                    # Insert abilities and trainer_pokemon_abilities
                    inserted_ids = []
                    for ability in abilities:
                        cursor.execute('''INSERT OR IGNORE INTO abilities (name) VALUES (?)''', (ability,))
                        cursor.execute('''SELECT id FROM abilities WHERE name = ?''', (ability,))
                        ability_id = cursor.fetchone()[0]

                        # Prevent duplicate
                        cursor.execute('''SELECT id FROM trainer_pokemon_abilities WHERE trainer_id = ? AND pokemon_id = ? AND ability_id = ?''', (trainer_id, pokemon_id, ability_id))
                        if not cursor.fetchone():
                            cursor.execute('''INSERT INTO trainer_pokemon_abilities (trainer_id, pokemon_id, ability_id) VALUES (?, ?, ?)''', (trainer_id, pokemon_id, ability_id))
                            inserted_ids.append(cursor.lastrowid)

                    return {"message": f"Abilities for {pokemon_name} added successfully.", "inserted_ids": inserted_ids}
        except HTTPException:
            raise
        except Exception as e:
            print(f"Internal Server Error: {e}")
            raise HTTPException(status_code=500, detail="Internal server error.")
    return app
        # --- End Implementation ---
# --- Main execution / Uvicorn setup (Optional - for candidate to run locally) ---
if __name__ == "__main__":
    # Ensure data is cleaned before running the app for testing
    temp_conn = connect_db()
    if temp_conn:
        clean_database(temp_conn)
        temp_conn.close()

    app_instance = create_fastapi_app()
    uvicorn.run(app_instance, host="127.0.0.1", port=8000)
