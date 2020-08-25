# -*- coding: iso-8859-15 -*-

import json
import time
import csv
import configparser
import requests
import logging
from tqdm import tqdm


def main():
    """ Hauptfunktion """
    # cache_tool = CacheTool('cleverreach.db')

    # Config aus config.ini laden
    config = configparser.ConfigParser()
    config.read("config.ini")

    BASE_URL = config["CLEVERREACH"]["BASE_URL"]
    LOGIN = config["CLEVERREACH"]["LOGIN"]
    PASSWORD = config["CLEVERREACH"]["PASSWORD"]
    CLIENT_ID = config["CLEVERREACH"]["CLIENT_ID"]
    CR_TOKEN = config["CLEVERREACH"]["TOKEN"]
    PAGESIZE = int(config["CLEVERREACH"]["PAGESIZE"])

    # Logging-Datei
    logging.basicConfig(filename="cleverreach_sicherung.log", level=logging.INFO)

    # Zu sichernde Gruppen aus groups.ini laden
    config_groups = configparser.ConfigParser()
    config_groups.read("groups.ini")

    groups = []
    for group_key in config_groups.keys():
        if not group_key == "DEFAULT":
            group_item = {
                "id": group_key,
                "last_saved": config_groups[group_key]["last_saved"],
            }
            groups.append(group_item)

    # DEBUG-Ausgaben
    debug = False

    logging.info("## CLEVERREACH-BACKUP")
    logging.info("# " + time.asctime())
    logging.info(" ... pagesize: " + str(PAGESIZE))
    logging.info(" ... check, ob Token noch gültig ist.")
    token_request = requests.get(BASE_URL + "/groups", {"token": CR_TOKEN})
    if token_request.status_code != 200:
        logging.info(
            "     ... Token ungültig. Hole einen neuen.", token_request.status_code
        )
        token_login_data = {
            "client_id": CLIENT_ID,
            "login": LOGIN,
            "password": PASSWORD,
        }
        token_request = requests.post(BASE_URL + "/login", token_login_data)
        CR_TOKEN = token_request.text.strip()
    else:
        logging.info(" ... Token gültigt.")

    # TOKEN-Anhängsel
    data_token = "?token=" + CR_TOKEN

    logging.info("Folgende Gruppen aus INI geholt:")
    for group in groups:
        logging.info(group["id"] + "; " + group["last_saved"])

    tmp_list_count = 0
    logging.info("Anzahl Gruppen: " + str(len(groups)))

    # Die Empfänger jeder Gruppe holen und speichern
    for group in groups:
        logging.info("# Gruppe: " + str(group["id"]))

        if time.strptime(group["last_saved"])[2] == time.strptime(time.asctime())[2]:
            continue

        tmp_list_count += 1

        # Statistiken holen
        # Vorallem für die Gesamtanzahl an Einträgen
        logging.info(" ... hole stats")
        item_stats_url = "/groups.json/{0}/stats{1}".format(group["id"], data_token)
        item_stats_encoded = json.loads(
            requests.get(BASE_URL + item_stats_url).text.strip()
        )
        if "error" in item_stats_encoded:
            logging.info("     ... Gruppe nicht vorhanden: " + str(group["id"]))
            print("Gruppe nicht vorhanden: ", group["id"])
            continue
        logging.info("     ... check")

        # Infos holen
        # Für den Gruppennamen
        logging.info(" ... hole infos")
        item_info_url = "/groups.json/{0}{1}".format(group["id"], data_token)
        item_info_encoded = json.loads(
            requests.get(BASE_URL + item_info_url).text.strip()
        )
        logging.info("     ... check")
        if "error" in item_info_encoded:
            logging.info("     ... Gruppe nicht vorhanden: " + str(group["id"]))
            print("Fehler bei group-ID ", group["id"])
            continue
        # print(item_info_encoded)
        group["name"] = item_info_encoded["name"]
        logging.info(" ... Gruppenname: " + str(group["name"]))

        # Seitenanzahl berechnen
        if (
            item_stats_encoded["total_count"] < PAGESIZE
            and not item_stats_encoded["total_count"] == 0
        ):
            item_pages = 1
        elif item_stats_encoded["total_count"] == 0:
            logging.info(" .. Gruppe leer")
            continue
        else:
            item_pages = item_stats_encoded["total_count"] // PAGESIZE + 1
        logging.info(" .. Länge der Liste: " + str(item_stats_encoded["total_count"]))
        logging.info(" .. Anzahl Seiten: " + str(item_pages))

        # CSV erstellen und Header schreiben
        filename = (
            "backups/"
            + group["name"]
            + "_"
            + time.strftime("%Y-%m-%d_%H-%M", time.localtime())
            + ".csv"
        )
        csv_file = csv.writer(open(filename, "w", newline=""))
        csv_file.writerow(
            [
                "id",
                "email",
                "activated",
                "registered",
                "source",
                "active",
                "global_attributes_name",
                "global_attributes_vorname",
                "global_attributes_brief_anrede",
            ]
        )
        logging.info(" .. CSV erstellt und geöffnet.")
        with tqdm(
            total=item_stats_encoded["total_count"],
            desc="Anzahl Einträge",
            unit=" Einträge",
        ) as pbar:
            item_url = "/groups.json/{0}/receivers".format(group["id"])
            for page_count in range(0, item_pages):
                # Jede Seite von Cleverreach laden
                # Wird nicht gecached!
                # item_return_json = cache_tool.request(BASE_URL+item_url+data_token+'&page='+str(page_count)+'&pagesize='+str(PAGESIZE))
                item_return_json = requests.get(
                    BASE_URL
                    + item_url
                    + data_token
                    + "&page="
                    + str(page_count)
                    + "&pagesize="
                    + str(PAGESIZE)
                ).text.strip()
                if debug:
                    print(
                        " .. Seite {0} von {1} geholt.".format(page_count, item_pages)
                    )
                item_return_encoded = json.loads(item_return_json)
                # Wenn die Liste nicht leer ist, wird sie in einer CSV gespeichert
                if debug:
                    print(" .. Länge der Liste: ", len(item_return_encoded))
                if len(item_return_encoded) > 0:
                    if debug:
                        print(" .. Seite bearbeiten.")
                    for reciever in item_return_encoded:
                        # print(" .. Reciever-eMail: ",reciever["email"])
                        csv_file.writerow(
                            [
                                reciever["id"],
                                reciever["email"],
                                reciever["activated"],
                                reciever["registered"],
                                reciever["source"],
                                reciever["active"],
                                reciever["global_attributes"]["name"],
                                reciever["global_attributes"]["vorname"],
                                reciever["global_attributes"]["briefanrede"],
                            ]
                        )
                        pbar.update(1)
                else:
                    # print(group['name'], item_stats_encoded['total_count'], len(item_return_encoded))
                    # print(item_url)
                    logging.info("Gruppe leer! " + str(group["name"]))
        logging.info(" ... csv gespeichert.")
        config_groups[group["id"]]["last_saved"] = time.asctime()

        logging.info(" ... done.")
        # if tmp_list_count == 1:
        #     print("# {0} Listen abgefragt.")
        #     break
    logging.info(" ... alle Gruppen gesichert.")
    with open("groups.ini", "w") as configfile:
        config_groups.write(configfile)

    logging.info(" ... Änderungen bei Gruppendaten gespeichert.")
    logging.info("## ENDE")


if __name__ == "__main__":
    main()
