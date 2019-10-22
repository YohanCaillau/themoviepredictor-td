#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
TheMoviePredictor script
Author: Arnaud de Mouhy <arnaud@admds.net>
"""

import mysql.connector
import sys
import argparse
import csv

def connectToDatabase():
    return mysql.connector.connect(user='predictor', password='predictor',
                              host='127.0.0.1',
                              database='predictor')

def disconnectDatabase(cnx):
    cnx.close()

def createCursor(cnx):
    return cnx.cursor(dictionary=True)

def closeCursor(cursor):    
    cursor.close()

def findQuery(table, id):
    return ("SELECT * FROM {} WHERE id = {}".format(table, id))

def findAllQuery(table):
    return ("SELECT * FROM {}".format(table))

def insert_people_query(firstname, lastname):
    return (f"INSERT INTO `people` (`firstname`, `lastname`) VALUES ('{firstname}', '{lastname}');")

def insert_movie_query(title, original_title, duration, rating, release_date):
    return (f"INSERT INTO `movies` (`title`, `original_title`, `duration`, `rating`, `release_date`) VALUES ('{title}', '{original_title}', {duration}, '{rating}', '{release_date}');")


def find(table, id):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    query = findQuery(table, id)
    cursor.execute(query)
    results = cursor.fetchall()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return results

def findAll(table):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    cursor.execute(findAllQuery(table))
    results = cursor.fetchall()
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return results

def insert_people(firstname, lastname):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    cursor.execute(insert_people_query(firstname, lastname))
    cnx.commit()
    last_id = cursor.lastrowid
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return last_id

def insert_movie(title, original_title, duration, rating, release_date):
    cnx = connectToDatabase()
    cursor = createCursor(cnx)
    cursor.execute(insert_movie_query(title, original_title, duration, rating, release_date))
    cnx.commit()
    last_id = cursor.lastrowid
    closeCursor(cursor)
    disconnectDatabase(cnx)
    return last_id

def printPerson(person):
    print("#{}: {} {}".format(person['id'], person['firstname'], person['lastname']))

def printMovie(movie):
    print("#{}: {} released on {}".format(movie['id'], movie['title'], movie['release_date']))

parser = argparse.ArgumentParser(description='Process MoviePredictor data')

parser.add_argument('context', choices=('people', 'movies'), help='Le contexte dans lequel nous allons travailler')

action_subparser = parser.add_subparsers(title='action', dest='action')

list_parser = action_subparser.add_parser('list', help='Liste les entitées du contexte')
list_parser.add_argument('--export' , help='Chemin du fichier exporté')

find_parser = action_subparser.add_parser('find', help='Trouve une entité selon un paramètre')
find_parser.add_argument('id' , help='Identifant à rechercher')

import_parser = action_subparser.add_parser('import', help='Importer un fichier CSV')
import_parser.add_argument('--file', help='Chemin vers le fichier à importer', required=True)

insert_parser = action_subparser.add_parser('insert', help='Insert une nouvelle entité')
known_args = parser.parse_known_args()[0]

if known_args.context == "people":
    insert_parser.add_argument('--firstname' , help='Prénom de la nouvelle personne', required=True)
    insert_parser.add_argument('--lastname' , help='Nom de la nouvelle personne', required=True)

if known_args.context == "movies":
    insert_parser.add_argument('--title' , help='Titre en France', required=True)
    insert_parser.add_argument('--duration' , help='Durée du film', type=int, required=True)
    insert_parser.add_argument('--original-title' , help='Titre original', required=True)
    insert_parser.add_argument('--release-date' , help='Date de sortie en France', required=True)
    insert_parser.add_argument('--rating' , help='Classification du film', choices=('TP', '-12', '-16'), required=True)



args = parser.parse_args()

if args.context == "people":
    if args.action == "list":
        people = findAll("people")
        if args.export:
            with open(args.export, 'w', encoding='utf-8', newline='\n') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(people[0].keys())
                for person in people:
                    writer.writerow(person.values())
        else:
            for person in people:
                printPerson(person)
    if args.action == "find":
        peopleId = args.id
        people = find("people", peopleId)
        for person in people:
            printPerson(person)
    if args.action == "insert":
        print(f"Insertion d'une nouvelle personne: {args.firstname} {args.lastname}")
        people_id = insert_people(firstname=args.firstname, lastname=args.lastname)
        print(f"Nouvelle personne insérée avec l'id '{people_id}'")

if args.context == "movies":
    if args.action == "list":  
        movies = findAll("movies")
        for movie in movies:
            printMovie(movie)
    if args.action == "find":  
        movieId = args.id
        movies = find("movies", movieId)
        for movie in movies:
            printMovie(movie)
    if args.action == "insert":
        print(f"Insertion d'un nouveau film: {args.title}")
        movie_id = insert_movie(
            title=args.title,
            original_title=args.original_title,
            duration=args.duration,
            rating=args.rating,
            release_date=args.release_date
        )
        print(f"Nouveau film inséré avec l'id '{movie_id}'")
    if args.action == "import":
        with open(args.file, 'r', encoding='utf-8', newline='\n') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                movie_id = insert_movie(
                    title=row['title'],
                    original_title=row['original_title'],
                    duration=row['duration'],
                    rating=row['rating'],
                    release_date=row['release_date']
                )
                print(f"Nouveau film inséré avec l'id '{movie_id}'")