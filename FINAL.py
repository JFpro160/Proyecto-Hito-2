import pandas as pd
from faker import Faker
import random
from datetime import datetime, date
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

fake = Faker()

# Cargar dataset de Pokémon
pokemon_df = pd.read_csv('pokemon.csv')
pokemon_df = pokemon_df[pokemon_df['generation'] <= 5]

elementos_traduccion = {
    'grass': 'Planta', 'fire': 'Fuego', 'water': 'Agua', 'bug': 'Bicho', 'normal': 'Normal',
    'poison': 'Veneno', 'electric': 'Eléctrico', 'ground': 'Tierra', 'fairy': 'Hada',
    'fighting': 'Lucha', 'psychic': 'Psíquico', 'rock': 'Roca', 'ghost': 'Fantasma',
    'ice': 'Hielo', 'dragon': 'Dragón', 'dark': 'Siniestro', 'steel': 'Acero', 'flying': 'Volador'
}

# Listas traducidas y necesarias
pokemon_items = {
    "Poción": "Poción", "Super Poción": "Poción", "Hiper Poción": "Poción", "Máxima Poción": "Poción",
    "Restaurar Todo": "Poción", "Revivir": "Poción", "Máximo Revivir": "Poción", "Agua Fresca": "Poción",
    "Refresco": "Poción", "Limonada": "Poción", "Leche Mu-mu": "Poción", "Polvo Energía": "Poción",
    "Raíz Energía": "Poción", "Polvo Curación": "Poción", "Hierba Revivir": "Poción", "Éter": "Poción",
    "Máximo Éter": "Poción", "Elixir": "Poción"
}

eventos = {
    "Sombrero Pikachu": {"fecha": date(2020, 10, 30), "evento": "Halloween 2020"},
    "Mochila Charizard": {"fecha": date(2021, 12, 24), "evento": "Christmas 2021"},
    "Gafas Squirtle": {"fecha": date(2019, 2, 7), "evento": "Lunar New Year 2019"},
    "Camiseta Bulbasaur": {"fecha": date(2022, 10, 30), "evento": "Halloween 2022"},
    "Disfraz Gengar": {"fecha": date(2023, 12, 24), "evento": "Christmas 2023"},
    "Capa Dragonite": {"fecha": date(2020, 1, 28), "evento": "Lunar New Year 2020"},
    "Sombrero Jigglypuff": {"fecha": date(2021, 1, 31), "evento": "Lunar New Year 2021"},
    "Máscara Lucario": {"fecha": date(2022, 1, 23), "evento": "Lunar New Year 2022"},
    "Zapatos Eevee": {"fecha": date(2023, 2, 13), "evento": "Lunar New Year 2023"},
    "Guantes Pikachu": {"fecha": date(2024, 2, 12), "evento": "Lunar New Year 2024"}
}

habilidades = [
    "Espesura", "Mar Llamas", "Torrente", "Polvo Escudo", "Mudar", "Vista Lince", "Fuga",
    "Agallas", "Intimidación", "Elec. Estática", "Velo Arena", "Punto Tóxico", "Gran Encanto",
    "Muro Mágico", "Ignorante", "Absorbe Fuego", "Sequía", "Manto Níveo", "Foco Interno",
    "Levitación", "Enjambre", "Técnico", "Inmunidad", "Sebo", "Corte Fuerte",
    "Efecto Espora", "Piel Seca", "Nado Rápido", "Clorofila", "Poder Solar", "Cura Lluvia",
    "Cromolente", "Pararrayos", "Ímpetu Arena", "Fuerza Bruta", "Absorbe Agua", "Absorbe Elec.",
    "Adaptable", "Encadenado", "Ojo Compuesto", "Velo Agua", "Escama Esp.", "Cabeza Roca", "Presión"
]

particles = [
    "Brillante", "Chispa", "Etéreo", "Encantado", "Sombra", "Confeti", "Fuegos Artificiales", "Arcoíris",
    "Otoño", "Invierno", "Primavera", "Verano", "Titileo", "Oscuridad", "Aura", "Brillo Secreto"
]

natures = [
    "Fuerte", "Huraña", "Audaz", "Firme", "Pícara", "Osada", "Dócil", "Plácida", "Agitada", "Floja",
    "Miedosa", "Activa", "Seria", "Alegre", "Ingenua", "Modesta", "Afable", "Mansa", "Tímida", "Alocada",
    "Serena", "Amable", "Grosera", "Cauta", "Rara"
]

valid_moves = [
    "Destructor", "Karate Chop", "Doble Bofetón", "Puño Cometa", "Puño Trueno", "Puño Fuego", "Puño Hielo",
    "Arañazo", "Torbellino", "Cuchillada", "Viento Cortante", "Danza Espada", "Corte", "Tajo Aéreo", "Ataque Ala",
    "Remolino", "Vuelo", "Atadura", "Placaje", "Pisotón", "Latigazo", "Doble Patada", "Patada Baja", "Patada Salto",
    "Patada Giro", "Ataque Rápido", "Ataque Furia", "Pin Misil", "Doble Equipo", "Día Soleado", "Rayo Solar",
    "Furia Dragón", "Danza Dragón", "Viento Aciago", "Rayo Hielo", "Viento Hielo", "Trueno", "Rayo", "Mordisco",
    "Rugido", "Canto", "Supersónico", "Bomba Sónica", "Deslumbrar", "Mordisco", "Colmillo Ígneo", "Colmillo Hielo",
    "Colmillo Rayo", "Corte Furia", "Picotazo", "Taladradora", "Golpe Roca", "Lanzarrocas", "Avalancha", "Viento Plata"
]


# Utility functions
def unique_names(n, name_list):
    generated_names = list(set(name_list))
    while len(generated_names) < n:
        generated_names.append(clean_name(fake.user_name()))  # Limpia los nombres generados
    return generated_names[:n]


def clean_name(name):
    return name.replace('"', '').replace("'", "").strip()


def generate_usuarios(n, name_list):
    used_emails = set()
    used_names = set(name_list)  # Inicializar con los nombres proporcionados
    usuarios = []

    # Utilizar nombres de la lista proporcionada primero
    for name in name_list:
        name = clean_name(name)  # Limpia el nombre aquí
        email = fake.email()
        while email in used_emails:
            email = fake.email()
        used_emails.add(email)
        used_names.add(name)
        usuarios.append({'nombre': name, 'correo': email})

    # Generar nombres únicos adicionales si es necesario
    while len(usuarios) < n:
        name = fake.user_name()
        while name in used_names:
            name = fake.user_name()
        email = fake.email()
        while email in used_emails:
            email = fake.email()
        used_emails.add(email)
        used_names.add(name)
        usuarios.append({'nombre': name, 'correo': email})

    return pd.DataFrame(usuarios)


def generate_personajes(n, usuarios):
    personajes = []
    generated_names = set()

    for usuario in usuarios:
        # Crear un personaje con el mismo nombre que el usuario
        nombre = usuario['nombre']
        generated_names.add(nombre)
        dinero = random.randint(0, 100000000)
        horas_jugadas = random.randint(0, 20000)
        fecha_creacion = fake.date_between(start_date='-5y', end_date='today')
        personajes.append({
            'nombre': nombre, 'dinero': dinero, 'horas_jugadas': horas_jugadas,
            'fecha_creacion': fecha_creacion, 'usuario': usuario['nombre']
        })
    return pd.DataFrame(personajes)
def generate_info_items(n, item_dict, eventos):
    info_items = []
    used_dates = set()

    # Generar ítems de pokemon_items
    for nombre, tipo in item_dict.items():
        fecha_salida = fake.date_between(start_date=date(2019, 1, 1), end_date=date(2019, 12, 31))
        while (nombre, fecha_salida) in used_dates:
            fecha_salida = fake.date_between(start_date=date(2019, 1, 1), end_date=date(2019, 12, 31))
        used_dates.add((nombre, fecha_salida))
        evento = "Ninguno"
        estacional = fake.boolean()
        limitado = fake.boolean()
        efectos_luz = fake.boolean()
        animaciones_extra = fake.boolean()
        ultra_raro = fake.boolean()
        coloreable = fake.boolean()
        info_items.append({
            'nombre': nombre, 'fecha_salida': fecha_salida, 'tipo': tipo, 'evento': evento,
            'estacional': estacional, 'limitado': limitado, 'efectos_luz': efectos_luz,
            'animaciones_extra': animaciones_extra, 'ultra_raro': ultra_raro, 'coloreable': coloreable
        })

    # Generar ítems de eventos
    for nombre, data in eventos.items():
        fecha_salida = data["fecha"]
        evento = data["evento"]
        tipo = "Cosmético"
        estacional = fake.boolean()
        limitado = fake.boolean()
        efectos_luz = fake.boolean()
        animaciones_extra = fake.boolean()
        ultra_raro = fake.boolean()
        coloreable = fake.boolean()
        info_items.append({
            'nombre': nombre, 'fecha_salida': fecha_salida, 'tipo': tipo, 'evento': evento,
            'estacional': estacional, 'limitado': limitado, 'efectos_luz': efectos_luz,
            'animaciones_extra': animaciones_extra, 'ultra_raro': ultra_raro, 'coloreable': coloreable
        })

    return pd.DataFrame(info_items)


def generate_items(n, personajes, info_items):
    items = []
    for _ in range(n):
        fecha_publicacion = fake.date_time_this_decade()
        vendedor = random.choice(personajes)['nombre']
        info_item = random.choice(info_items)
        nombre = info_item['nombre']
        fecha_salida = info_item['fecha_salida']
        precio = random.randint(1, 100000000)
        stock = random.randint(0, 10000)
        en_stock = stock > 0 if random.random() > 0.1 else False
        items.append({
            'fecha_publicacion': fecha_publicacion, 'vendedor': vendedor, 'nombre': nombre,
            'fecha_salida': fecha_salida,
            'precio': precio, 'stock_original': stock
        })
    return pd.DataFrame(items)


def generate_compra_items(n, items, personajes):
    compra_items = []
    for _ in range(n):
        fecha_compra = fake.date_time_this_decade()
        item = random.choice(items)
        fecha_publicacion = item['fecha_publicacion']
        vendedor = item['vendedor']
        comprador = random.choice(personajes)['nombre']
        while comprador == vendedor:
            comprador = random.choice(personajes)['nombre']
        cantidad_comprada = random.randint(1, 1000)
        compra_items.append({
            'fecha_compra': fecha_compra, 'comprador': comprador, 'fecha_publicacion': fecha_publicacion,
            'vendedor': vendedor, 'cantidad_comprada': cantidad_comprada
        })
    return pd.DataFrame(compra_items)


def generate_info_pokemons(pokemon_df):
    egg_groups = [
        'Monstruo', 'Agua 1', 'Insecto', 'Volador',
        'Campo', 'Hada', 'Planta', 'Humanoide',
        'Agua 3', 'Mineral', 'Amorfo', 'Agua 2',
        'Ditto', 'Dragón', 'Desconocido'
    ]
    tiers = ["UU", "NU", "OU", "UB", "ST"]
    info_pokemons = []
    for _, row in pokemon_df.iterrows():
        grupo_huevo = random.choice(egg_groups)
        tier = random.choice(tiers)
        info_pokemons.append({
            'nombre': row['name'], 'grupo_huevo': grupo_huevo, 'tier': tier,
            'es_legendario': row['is_legendary']
        })
    return pd.DataFrame(info_pokemons)


def generate_elementos(pokemon_df):
    elementos = []
    for _, row in pokemon_df.iterrows():
        elemento1 = elementos_traduccion.get(row['type1'], row['type1'])
        elementos.append({'pokemon': row['name'], 'elemento': elemento1})
        if pd.notna(row['type2']) and row['type2'] != row['type1']:
            elemento2 = elementos_traduccion.get(row['type2'], row['type2'])
            elementos.append({'pokemon': row['name'], 'elemento': elemento2})
    return pd.DataFrame(elementos)


def generate_pokemons(n, personajes, info_pokemons):
    pokemons = []
    for _ in range(n):
        fecha_publicacion = fake.date_time_this_decade()
        vendedor = random.choice(personajes)['nombre']
        info_pokemon = random.choice(info_pokemons)
        nombre = info_pokemon['nombre']
        precio = random.randint(1, 100000000)
        en_stock = fake.boolean()
        sexo = random.choice(['M', 'F'])
        naturaleza = random.choice(natures)
        habilidad = random.choice(habilidades)
        tiene_habilidad_oculta = fake.boolean()
        es_alpha = fake.boolean()
        anho_captura = random.randint(2019, 2024)
        nivel = random.randint(0, 100)
        iv_ps = random.randint(0, 31)
        iv_ataque = random.randint(0, 31)
        iv_defensa = random.randint(0, 31)
        iv_at_esp = random.randint(0, 31)
        iv_def_esp = random.randint(0, 31)
        iv_velocidad = random.randint(0, 31)
        ev_ps = random.randint(0, 252)
        ev_ataque = random.randint(0, 252)
        ev_defensa = random.randint(0, 252)
        ev_at_esp = random.randint(0, 252)
        ev_def_esp = random.randint(0, 252)
        ev_velocidad = random.randint(0, 252)
        total_evs = ev_ps + ev_ataque + ev_defensa + ev_at_esp + ev_def_esp + ev_velocidad
        if total_evs > 510:
            ev_ps = abs(ev_ps - 176)
            ev_ataque = abs(ev_ataque - 176)
            ev_defensa = abs(ev_defensa - 176)
            ev_at_esp = abs(ev_at_esp - 176)
            ev_def_esp = abs(ev_def_esp - 176)
            ev_velocidad = abs(ev_velocidad - 176)

        pokemons.append({
            'fecha_publicacion': fecha_publicacion, 'vendedor': vendedor, 'nombre': nombre, 'precio': precio,
            'en_stock': en_stock, 'sexo': sexo, 'naturaleza': naturaleza, 'habilidad': habilidad,
            'tiene_habilidad_oculta': tiene_habilidad_oculta, 'es_alpha': es_alpha, 'anho_captura': anho_captura,
            'nivel': nivel, 'iv_ps': iv_ps, 'iv_ataque': iv_ataque, 'iv_defensa': iv_defensa, 'iv_at_esp': iv_at_esp,
            'iv_def_esp': iv_def_esp, 'iv_velocidad': iv_velocidad, 'ev_ps': ev_ps, 'ev_ataque': ev_ataque,
            'ev_defensa': ev_defensa, 'ev_at_esp': ev_at_esp, 'ev_def_esp': ev_def_esp, 'ev_velocidad': ev_velocidad
        })
    return pd.DataFrame(pokemons)


def generate_particulas(n, pokemons):
    particulas = []
    used_times = set()
    for _ in range(n):
        pokemon = random.choice(pokemons)
        fecha_publicacion = pokemon['fecha_publicacion']
        vendedor = pokemon['vendedor']
        while (fecha_publicacion, vendedor) in used_times:
            pokemon = random.choice(pokemons)
            fecha_publicacion = pokemon['fecha_publicacion']
            vendedor = pokemon['vendedor']
        used_times.add((fecha_publicacion, vendedor))
        particula = random.choice(particles)
        particulas.append({'fecha_publicacion': fecha_publicacion, 'vendedor': vendedor, 'particula': particula})
    return pd.DataFrame(particulas)


def generate_movimientos(n, pokemons, moves):
    movimientos = []
    used_combinations = {}
    all_moves = set(moves)

    for pokemon in pokemons:
        fecha_publicacion = pokemon['fecha_publicacion']
        vendedor = pokemon['vendedor']

        key = (fecha_publicacion, vendedor)
        if key not in used_combinations:
            used_combinations[key] = set()

        # Calcular el número de movimientos a agregar, asegurándose de que no exceda 4
        max_moves = 4 - len(used_combinations[key])
        if max_moves > 0:
            available_moves = list(all_moves - used_combinations[key])
            num_moves = random.randint(1, max_moves)
            moves_for_pokemon = random.sample(available_moves, num_moves)
            for movimiento in moves_for_pokemon:
                used_combinations[key].add(movimiento)
                movimientos.append({
                    'fecha_publicacion': fecha_publicacion, 'vendedor': vendedor, 'movimiento': movimiento
                })
    return pd.DataFrame(movimientos)


def generate_compra_pokemons(n, pokemons, personajes):
    compra_pokemons = []
    used_dates = set()

    for _ in range(n):
        fecha_compra = fake.date_time_this_decade()
        pokemon = random.choice(pokemons)
        fecha_publicacion = pokemon['fecha_publicacion']
        vendedor = pokemon['vendedor']

        # Asegurarse de que la fecha de publicación sea única
        while (fecha_publicacion, vendedor) in used_dates:
            pokemon = random.choice(pokemons)
            fecha_publicacion = pokemon['fecha_publicacion']
            vendedor = pokemon['vendedor']

        used_dates.add((fecha_publicacion, vendedor))

        comprador = random.choice(personajes)['nombre']
        while comprador == vendedor:
            comprador = random.choice(personajes)['nombre']

        compra_pokemons.append({
            'fecha_compra': fecha_compra, 'comprador': comprador, 'fecha_publicacion': fecha_publicacion,
            'vendedor': vendedor
        })

    return pd.DataFrame(compra_pokemons)


# Generate and save data while respecting foreign key constraints
def save_data(n, prefix):
    num_cores = multiprocessing.cpu_count()

    with ThreadPoolExecutor(max_workers=num_cores) as executor:
        # Step 1: Generate usuarios
        usuarios_future = executor.submit(generate_usuarios, n, [
            "Aguinaga Pizarro Bianca Brunella", "Aguilar Delgado Segundo Fernando",
            "Anccana Llanto Nicol Valery Abigail", "Astorga Taccsi Alexander Antonio",
            "Ayra Francisco Gussephe Benjamin", "Baltazar Palomino Saul Alejandro",
            "Boza Martin Adrián Anthoni", "Bravo Valdivia Carlos Raith",
            "Caballero Sarco Humberto Daniel", "Cabezas Ramirez Dylan Andres",
            "Cama Ardiles Santiago Aldebaran", "Camasca Tapara Jhon David",
            "Campuzano Diestra Leonel Leodolfo", "Capuñay Lluen Fabian Higinio",
            "Castro Vergara Rodrigo Alonso", "Catacora Tupa Alair Jairo",
            "Centeno Rosas Efrén Paolo", "Chancafe Gonzales Marcelo Julian",
            "Chipayo Flores Alejandro Sebastian", "Claros Montero Daniel Esteven",
            "Colque Ordoñez Patrick Fabrizio", "Colquepisco Acevedo Alexanderh",
            "Condor Taype Lucero Rocio", "Condori Flores Ian Kevin",
            "Cornejo Santivañez Jamin Arnold", "Coveñas Rojas Camila Milagros",
            "De la Cruz Huaman Jose Carlos", "Dulanto Fierro Kevin Richard",
            "Escobar Nuñez Alejandro Ismael", "Espinoza Cabrera Camila Del Rosario",
            "Espinoza Monti Mateo", "Fabricio Gabriel Bautista Vereau",
            "Flores Godos Carlos Gabriel", "Flores Uriarte Cesar Stefano",
            "Galvez Pacori Jose Guillermo", "Garcia Cacñahuaray Alexis Jeremi",
            "Garcia Calle Renato", "Gomez Villegas Enzo Sebastian",
            "Gonzalez Jara Alvaro David", "Huaman Casma Leonardo Fabian",
            "Huamán García Joaquin Ernesto", "Huaroc Enciso Rodolfo Elard",
            "Huaylla Huillca Jean Piero", "Illescas Castillo Diego Roger",
            "Inga Quispe Grace Kelly", "Iribar Casanova Federico",
            "Jesus Herrera Mark Antony", "Lazaro Condor Chiara Olenka",
            "Leon Villarreyes Jorge Alexander", "Li Chumpitaz Rodrigo",
            "Lizardo Mejia Zamir Rogger", "Lopez Carrera Brando Steve",
            "Lopez Gambini Elvis Alejandro", "Luis Fernando Lopez Chambi",
            "Luque Segura Juan Diego", "Maquera Bobadilla Diva Stewart",
            "Marcelo Chinchay Alejandro Jesús", "Medianero Euscate Fernando Miguel",
            "Medrano Teran Paolo", "Mendez Moncada Danna Valeria",
            "Mendoza Navarro Emerson Ademir", "Meneses Roncal Matías Alonso",
            "Miguel Fernandez Joel David", "Miranda Santillán Richard Aldahir",
            "Muñoz Zamora Alexander Rafael", "Ñañez Andrés Diana Carolina",
            "Padilla Vela Fernando Felix", "Paredes Alva Juan Carlos Daniel",
            "Pastor Díaz Renzo Adrian", "Perales Chávez Nicolas Alberto",
            "Pizarro Jimenez Alvaro Mauricio", "Ponce de León Vargas Héctor Augusto",
            "Povis Reyes Brandon Brayan", "Quenta Solis Jorge Eduardo",
            "Quispe Gonzales Fabrizio Sebastián", "Quispe Yauri Rosario Lizeth",
            "Ramírez Chuquimarca Mateo Elías", "Reyes Velarde Piero Edmundo",
            "Reyna Gil Josue Lucio Jan", "Robledo Chuquicusma Luis Brauns",
            "Rodriguez Galarza Gonzalo Alejandro", "Romero Pahuara Sebastian",
            "Roque Castillo Franco Nicolás", "Salazar Meza Leslie Allison",
            "Samanez Quintana Sebastián Ignacio", "Silva Rios Alessandra Valeria",
            "Sulca Infante Esteban Jose", "Tataje Diaz Erick Jose",
            "Timaná Castro Aarón Víctor Manuel", "Ucañani Tintaya Miguel Fernando",
            "Valdivia Ccahuana Christopher Alberto", "Valero Perez Yesenia Mayumi",
            "Vilela Obando Yuri Fabian", "Villarreal Falcón Mishelle Stephany",
            "Villón Zárate Fátima Margarita", "Wisdom Pestana Axl",
            "Yalico Arango William Eduardo", "Zumba Alayo Carlos Rodrigo",
            "Zúñiga Nole Gilmar Gerardo", "Nagamine Oshiro Laura Gabriela",
            "Acuña Faustino Ana Maria", "Cotera Inche Antonio",
            "Bracamonte Toguchi Mikel Dan", "Lezama Orihuela Sergio Sebastian",
            "Suarez Torres Gonzalo Daniel", "Hinojosa Bittrich Michael Paul",
            "Melgarejo Castillo Jorge Eduardo", "Paca Sotero Jose Francisco",
            "Nieto Paz Héctor Sebastian", "Espinoza Salazar Granit",
            "Rodríguez Ramírez Badi Masud", "Aguilar Millones Jose Ignacio",
            "Escalante Ccoyllo Nahía Alejandra", "Gallegos Chavez Rodrigo Edersson",
            "Aragon Ayala Eduardo Fernando", "Sobenes Obregon Carlos Sebastian",
            "Inca Acuña Juan Rodolfo", "Mercado Barbieri Ariana Valeria", "Teofilo Chambilla"
        ])
        usuarios_df = usuarios_future.result()
        usuarios_df.to_csv(f'{prefix}_usuarios.csv', index=False)

        # Step 2: Generate personajes
        personajes_future = executor.submit(generate_personajes, n * 3, usuarios_df.to_dict('records'))
        personajes_df = personajes_future.result()
        personajes_df.to_csv(f'{prefix}_personajes.csv', index=False)

        # Step 3: Generate info items
        info_items_future = executor.submit(generate_info_items, n, pokemon_items, eventos)
        info_items_df = info_items_future.result()
        info_items_df.to_csv(f'{prefix}_info_items.csv', index=False)

        # Step 4: Generate items
        items_future = executor.submit(generate_items, n, personajes_df.to_dict('records'),
                                       info_items_df.to_dict('records'))
        items_df = items_future.result()
        items_df.to_csv(f'{prefix}_items.csv', index=False)

        # Step 5: Generate compra items
        compra_items_future = executor.submit(generate_compra_items, n, items_df.to_dict('records'),
                                              personajes_df.to_dict('records'))
        compra_items_df = compra_items_future.result()
        compra_items_df.to_csv(f'{prefix}_compra_items.csv', index=False)

        # Step 6: Generate info pokemons
        info_pokemons_future = executor.submit(generate_info_pokemons, pokemon_df)
        info_pokemons_df = info_pokemons_future.result()
        info_pokemons_df.to_csv(f'{prefix}_info_pokemons.csv', index=False)

        # Step 7: Generate elementos
        elementos_future = executor.submit(generate_elementos, pokemon_df)
        elementos_df = elementos_future.result()
        elementos_df.to_csv(f'{prefix}_elementos.csv', index=False)

        # Step 8: Generate pokemons
        pokemons_future = executor.submit(generate_pokemons, n, personajes_df.to_dict('records'),
                                          info_pokemons_df.to_dict('records'))
        pokemons_df = pokemons_future.result()
        pokemons_df.to_csv(f'{prefix}_pokemons.csv', index=False)

        # Step 9: Generate particulas
        particulas_future = executor.submit(generate_particulas, n, pokemons_df.to_dict('records'))
        particulas_df = particulas_future.result()
        particulas_df.to_csv(f'{prefix}_particulas.csv', index=False)

        # Step 10: Generate movimientos
        movimientos_future = executor.submit(generate_movimientos, n, pokemons_df.to_dict('records'), valid_moves)
        movimientos_df = movimientos_future.result()
        movimientos_df.to_csv(f'{prefix}_movimientos.csv', index=False)

        # Step 11: Generate compra pokemons
        compra_pokemons_future = executor.submit(generate_compra_pokemons, n, pokemons_df.to_dict('records'),
                                                 personajes_df.to_dict('records'))
        compra_pokemons_df = compra_pokemons_future.result()
        compra_pokemons_df.to_csv(f'{prefix}_compra_pokemons.csv', index=False)

    print('Data generation complete')


# Generate data for different sizes
# save_data(1000, '1000')
save_data(10000, '10000')
# save_data(100000, '100000')
# save_data(1000000, '1000000')
