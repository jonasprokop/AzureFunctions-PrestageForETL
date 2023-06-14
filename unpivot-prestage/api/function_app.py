import azure.functions as func
import logging
import pandas as pd
import io
import re
import openpyxl 
import numpy as np
import xlsxwriter
from azure.storage.blob import BlobClient
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential
import json
import datetime

# pripraveny kod pro napojeni azure key vault
# key_vault_name = ""
# def get_con_string(key_vault_name):
    # KVUri = f"https://{key_vault_name}.vault.azure.net"
    # credential = DefaultAzureCredential()
    # client = SecretClient(vault_url=KVUri, credential=credential)

    # secret_name = ""
    # con_string_blob_storage = client.get_secret(secret_name)
    # return con_string_blob_storage


"""uroven 1"""
# hlavni funkce uplatnujici se ve funkci main

# zajistuje nacteni json konfigurace
def load_json_configuration(con_string_blob_storage):

    # stazeni json konfigurace z blob storage
    blob_client_json = BlobClient.from_connection_string(
        con_string_blob_storage,
        container_name="json-konfigurace",
        blob_name="vykony_konf.json")
    downloader_json = blob_client_json.download_blob()
    logging.info("Script odeslal pozadavek na stazeni souboru s json konfiguraci:") 

    # object downloader musi byt preveden do bitove formy pro dalsi operace
    stream_json = downloader_json.content_as_bytes()

    # prevedeni json souboru na slovnik
    decoded = stream_json.decode("UTF-8")
    loaded_json_config = json.loads(decoded)
    logging.info("Byl nacten soubor json se vstupnim nastavenim operaci")
    
    # nacteni konfigurace
    json_config_all_tables = loaded_json_config["tabulky_info"]
    json_config_table_names = list(json_config_all_tables)
    json_config_export_info = loaded_json_config["export_info"]
    final_test_flag = json_config_export_info["final"]
    fis_vse_flag = json_config_export_info["fis"]
    export_date = json_config_export_info["datum_exportu"]
    container_import = json_config_export_info["container_import"]
    container_export = json_config_export_info["container_export"]
    container_log = json_config_export_info["container_log"]
    delete_input_files_flag = json_config_export_info["odstranit_vstupni_soubory"]

    loaded_json_config = [json_config_all_tables, json_config_table_names, final_test_flag, fis_vse_flag, export_date, container_import, 
                          container_export, container_log, delete_input_files_flag]
    
    logging.info("Konfigurace byla uspesne nactena")
    
    return loaded_json_config

# zajistuje nacteni vsech souboru ve vstupnim ulozisti blob
def list_all_files_in_blob(con_string_blob_storage, loaded_json_config):

    # nacteni konfigurace
    container_import = loaded_json_config[5]

    # tvorba blob storage service clienta
    blob_service_client = BlobServiceClient.from_connection_string(con_string_blob_storage)

    # tvorba container blob storage clienta
    container_client = blob_service_client.get_container_client(container_import)

    # tvorba seznamu souboru v containeru blob storage list_of_all_files_in_blob
    logging.info("Script odeslal pozadavek na stazeni seznamu souboru blob ve vstupnim blob storage:") 
    list_of_all_files_in_blob = container_client.list_blobs()
    list_of_all_files_in_blob = list(list_of_all_files_in_blob)
    logging.info("Script uspesne stahl seznam souboru ulozenych v blob storage a nyní jimi bude listovat a aplikovat dilci operace...") 
    return list_of_all_files_in_blob

# zastresuje process_table_extract_headings_by_json, process_table_extract_headings_dynamically a file_not_recognised_as_table_to_be_processed
# orchestruje a zaznamenava zpracovani souboru nactenych funkci list_all_files_in_blob
# je rozvetvena do dalsich 6 urovni az konci na uroveni exportu tabulek a generovani logu
def process_all_files_in_blob(con_string_blob_storage, loaded_json_config, list_of_all_files_in_blob):
    
    # seznam pro vystupni data z funkci process_table
    log_list = []
    
    # pocitadlo pro tabulky zpracovane podle jsonu
    tables_by_json = 0
    
    # pocitadlo a seznam pro tabulky zpracovane dynamicky
    names_dynamically = []
    tables_dynamically = 0

    # pocitadlo a seznam pro nerozpoznane soubory
    names_not_recognised = []
    files_not_recognised = 0

     # listovani soubory v seznamu list_of_all_files_in_blob a aplikovani dilcich funkci
    for blob in list_of_all_files_in_blob:
        logging.info(f"Script nacetl soubor: {blob['name']}") 
        
        # vnejsi try a except konstrukt, nejprve zkousi zpracovani pomoci process_table, pri selhani ulozi informace o souboru

        # zpracovani pomoci process_table
        try:
            
            # vnitrni try a except konstrukt, nejprve zkousi zpracovat soubor s nactenim podle json konfigurace, pri selhani zkousi zpracovani s dynamickym nacitanim zahlavi
            
            # zpracovani tabulky s nacitanim zahlavi podle json konfigurace
            try:
                
                # funkce process_table_extract_headings_by_json uklada vystup do slovniku log_list
                log_single_table_list = process_table_extract_headings_by_json(con_string_blob_storage, blob, loaded_json_config)
                first_dict = log_single_table_list[0]
                name_of_table = first_dict["Exportni jmeno"]
                logging.info(f"Tabulka {name_of_table} ({blob['name']}) byla uspesne upravena podle json konfigurace ")
                
                # dilci slovnik log_list je pridan do celkoveho seznamu slovniku log_list, tj. vstupniho souboru pro funkci create_metadata
                for log_single_table_dict in log_single_table_list:
                    log_single_table_dict.update({"Zahlavi nacteno":"Dle jsonu"})
                log_list += log_single_table_list

                # vstupni tabulka je odstranena

                # zaznam uspechu
                tables_by_json += 1
                
            # zpracovani tabulky s nacitanim zahlavi dynamicky
            except:
                
                # funkce process_table_extract_headings_dynamically uklada vystup do slovniku log_list
                log_single_table_list = process_table_extract_headings_dynamically(con_string_blob_storage, blob, loaded_json_config)
                first_dict = log_single_table_list[0]
                name_of_table = first_dict["Exportni jmeno"]
                logging.info(f"Tabulka {name_of_table} ({blob['name']}) byla uspesne upravena dynamicky")
                
                # dilci slovnik log_list je pridan do celkoveho seznamu slovniku log_list, tj. vstupniho souboru pro funkci create_metadata
                for log_single_table_dict in log_single_table_list:
                    log_single_table_dict.update({"Zahlavi nacteno":"Dynamicky"})
                log_list += log_single_table_list

                # zaznam uspechu
                tables_dynamically += 1
                names_dynamically += [blob['name']]


        # obe funkce process_table selhali, soubor je vynecham a je zaznamenano selhani pomoci unrecognised_file_log_list
        except:
            logging.info(f"Soubor {blob['name']} nebyl rozpoznan a byl z operace vynechan")
            unrecognised_file_log_list = file_not_recognised_as_table_to_be_processed(blob)
            log_list += unrecognised_file_log_list
            # zaznam selhani
            files_not_recognised += 1
            names_not_recognised += [blob['name']]
            

    return log_list, tables_by_json, names_dynamically, tables_dynamically, names_not_recognised, files_not_recognised

# vytvari metadata a shrnuje log, obsahuje funkce create_log, create_meatadata_file a generate_final_message
def create_metadata(con_string_blob_storage, loaded_json_config, processed_files):

    # nacteni konfigurace
    container_export = loaded_json_config[6]  
    container_log = loaded_json_config[7] 
    log_list = processed_files[0]

    logging.info("Script nyni generuje souhrnny log...")

    # tvorba logu
    general_log_dataframe = create_log(con_string_blob_storage, log_list, container_log)
    
    # tvorba souboru s metadaty
    logging.info(f"Script vytvari soubor s metadaty...")
    create_metadata_file(general_log_dataframe, con_string_blob_storage, container_export)

    # tvorba zaverecne zpravy exportovane do api
    message, fail_flag  = generate_final_message(processed_files)

    return message, fail_flag

# funkce ktera odstranuje vsechny soubory ve vstupnim ulozisti blob po uspesnem zpracovani
def delete_all_files_in_blob(con_string_blob_storage, loaded_json_config, list_of_all_files_in_blob, fail_flag, message):

    # nacteni konfigurace 
    container_import = loaded_json_config[5]
    delete_input_files_flag = loaded_json_config[8]

    # finalni odstraneni vstupnich souboru je podmineno nastavenim v json konfiguraci a spravnym nactenim vsech vstupnich souboru
    if fail_flag and delete_input_files_flag in ["ano", "Ano", "Yes", "yes", True]:
        logging.info("Vstupni soubory jsou urceny ke smazani..")
        # listovani seznamem blobu a mazani
        for blob in list_of_all_files_in_blob:
            logging.info(f"Vstupni soubor {blob['name']} bude odstranen..")

            blob_client = BlobClient.from_connection_string(
                    con_string_blob_storage,
                    container_name=container_import,
                    blob_name=blob['name'],
                )
            
            blob_client.delete_blob()
            logging.info(f"Vstupni soubor {blob['name']} byl odstranen")
        
        message = message + ", a vsechny tabulky byly po dokonceni operaci uspesne odstraneny ze vstupni storage"

    return message

"""uroven 2"""

# vnitrni funkce create_metadata

# tvorba souhrnneho logu operace ve formatu xlsx
def create_log(con_string_blob_storage, log_list, container_log):

    # tvorba logu operace
    # funkce vytvari pandas dataframe ze seznamu slovniku vystupu dvou predeslych funkci
    general_log_dataframe = pd.DataFrame(log_list)
    logging.info("Script vytvoril tabulku se souhrnym logem")
    logging.info(general_log_dataframe)

    # stanoveni casu ulozeni logu
    now = datetime.datetime.now()
    datetime.datetime(2009, 1, 6, 15, 8, 24, 78915)
    time_of_operation = str(now)
    
    # odstraneni mezer z casu ulozeni logu
    time_of_operation_pure = "".join(time_of_operation.split())
        
    logging.info(f"Cas zalogovani operace: {time_of_operation_pure}")

    # tvroba jmena log souboru
    name_of_log_file = "log" + "_" + time_of_operation_pure + ".xlsx"

    # tvorba exportniho clientu pro log soubor
    blob_service_client = BlobServiceClient.from_connection_string(con_string_blob_storage)           
    blob_service_client_export_log = blob_service_client.get_blob_client(container=container_log, blob=name_of_log_file)
    logging.info("Byl vytvoren exportni client azure blob service pro log soubor")
    table_name = "Souhrnny log"

    # vola exportni funkci pro nahrani souboru do slozky se souhrnymi logy operace 
    export_virtual_excel_file(general_log_dataframe, table_name, name_of_log_file, blob_service_client_export_log)

    return general_log_dataframe

# tvorba souboru s metadaty
def create_metadata_file(general_log_dataframe, con_string_blob_storage, container_export):

    # tvorba seznamu metadat s unikatnimi kody datasetu, zachovava se vzdy prvni nalezena kombinace 
    # (pro dany export by pak měli byt dalsi informace identicke, tato metoda se jevi nejjednodussi pro zajisteni nacitani dat rovnou z dilcich log_dict a ne ze zahlavi)
    dataframe_of_unique_dataset_id_combinations = general_log_dataframe.drop_duplicates(subset=["Kod datasetu"], keep="first", inplace=False)
    logging.info(dataframe_of_unique_dataset_id_combinations)
    codes_recognised_to_print = general_log_dataframe["Kod datasetu"].unique()
    logging.info(codes_recognised_to_print)

    # seznam pro pridani dilcich slovniku s finalnimi metadaty
    metadata_dataset_info_list = []
    
    # dataset unikatnich kombinaci je procitan a jednotlive kombinace jsou zaznameny do slovniku metadat
    for index, row in dataframe_of_unique_dataset_id_combinations.iterrows():

        # ulozeni kodu na zacatku operace
        code = row["Kod datasetu"]

        # pokud nebyl kod datasetu vygenerovan, tj. file nebyl nacten, neni tato kombinace zaznamenana
        if code != "":

            # vsechna data jsou nacitana primo z vystupnich, je tak zajistena jejich reprezentativnost
            fixace_dat = row["Fixace dat"]
            fis_code = row["Fis"]
            final_code = row["Final"]
            export_date = row["Datum exportu"]
            semestr = row["Semestr"]
            dose_code = row["Kod davky"]

            # kod FINAL a FIS jsou prevedeny do ciselne podoby
            if fis_code == "FIS":
                fis_code = 1
            else:
                fis_code = 0
            if final_code  == "FINAL":
                final_code = 1
            else:
                final_code = 0 
            
            # pro kazdy unikatni kod datasetu generujeme dilci slovnik 
            metadata_dataset_info_dict = {"Kód datasetu": code, "Kód dávky": dose_code, "Semestr datasetu":semestr, "Datum fixace":fixace_dat, "Datum exportu":export_date, "Finalni":final_code, "FIS": fis_code}
            logging.info(metadata_dataset_info_dict)
            metadata_dataset_info_list.append(metadata_dataset_info_dict)
        
    # ze seznamu s metadaty generujeme pandas dataframe
    metadata_frame = pd.DataFrame(metadata_dataset_info_list)
    logging.info(f"Vsechny datasety byly zaneseny do tabulky s metadaty, s kody datasetu: {codes_recognised_to_print}")

    
    # jmeno souboru s metadaty
    name_of_metadata_file = "metadata.xlsx"

    # tvorba exportniho clientu pro metadata soubor
    blob_service_client = BlobServiceClient.from_connection_string(con_string_blob_storage)    
    blob_client_export_metadata_file = blob_service_client.get_blob_client(container=container_export, blob=name_of_metadata_file)
    logging.info("Byl vytvoren exportni client azure blob service pro soubor s metadaty")
    table_name = "s metadaty"

    # vola exportni funkce pro nahrani souboru do vystupniho azure blob storage
    export_virtual_excel_file(metadata_frame, table_name, name_of_metadata_file, blob_client_export_metadata_file)

# funkce ktera vytvari zaverecnou zpravu,
def generate_final_message(processed_files):

    # nactenni promennych
    tables_by_json = processed_files[1]
    names_dynamically = processed_files[2]
    tables_dynamically = processed_files[3]
    names_not_recognised = processed_files[4]
    files_not_recognised = processed_files[5]

    # indikator toho, zda script uspesne zpracoval vsechny soubory
    all_files_proccesed = bool

    # pokud je zaznameno celkove selhani u nektereho ze souboru script vypise zpravu specifikujici toto selhani, vypsany jsou take specifikace tabulek zpracovanych dynamicky
    if files_not_recognised != 0:
        all_files_proccesed = False
        if tables_dynamically != 0:
            message = f"Transformace nebyla provedena uspesne, pocet tabulek zpracovanych podle json konfigurace: {tables_by_json}, pocet tabulek zpracovanych dynamicky: {tables_dynamically}, s relativnimi cestami: {names_dynamically}, pocet tabulek ktere nebyly ropoznany: {files_not_recognised}, s relativnimi cestami: {names_not_recognised}"
        else:
            message = f"Transformace nebyla provedena uspesne, pocet tabulek zpracovanych podle json konfigurace: {tables_by_json}, pocet tabulek ktere nebyly ropoznany: {files_not_recognised}, s relativnimi cestami: {names_not_recognised}"

    # pokud script spravne zpracuje vsechny tabulky podle jsonu vypise pocet zpracovanych tabulek, pokud nejake zpracuje take dynamicky vypise navic jejich specifikace
    else:
        all_files_proccesed = True
        message = f"Transformace byla provedena uspesne, pocet tabulek upravenych podle json kofigurace: {tables_by_json}"
        if tables_dynamically != 0:
            message += f", a pocet tabulek zpracovanych dynamicky: {tables_dynamically}, s relativnimi cestami: {names_dynamically}"

    logging.info(f"Script dokoncil operace a nyni shrne svou cinnost: {message}")

    return message, all_files_proccesed


# vnitrni funkce process_all_files_in_blob

# zpracovava jednu tablku se zahlavnim rozpoznanym podle json konfigurace, vystupem je slovnik s metadaty
def process_table_extract_headings_by_json(con_string_blob_storage, blob, loaded_json_config):
    logging.info("Tabulka bude nejprve zpracovana podle json konfigurace...")

    # nacteni konfigurace
    json_config_all_tables = loaded_json_config[0]
    json_config_table_names = loaded_json_config[1]
    container_import = loaded_json_config[5]

    # promenne
    headings = pd.DataFrame
    log_list = list

    # stazeni vstupniho streamu
    stream = load_stream(con_string_blob_storage, container_import, blob)

    # nacteni tabulky zahlavi
    headings = load_headings(stream)
    table_name = str(headings.iloc[0, 0])

    # pro dalsi zpracovani je nutne overit, zda je tabulka obsazena v json konfiguraci
    if table_name in json_config_table_names:
        logging.info(f"Tabulka {table_name} je v seznamu tabulek v json konfiguraci")

        # nacteni json konfigurace pro danou tabulku
        json_config_single_table = json_config_all_tables[table_name]
        logging.info(f"Tabulka {table_name} byla nactena")

        # nacteni informaci z tabulky zahlavi podle json konfigurace
        extracted_headings = extract_headings_by_json(table_name, json_config_single_table, headings)
        logging.info(f"Script uspesne overil, ze nactena pole tabulky {table_name} odpovidaji formatu typickemu pro dany typ informace")

        # zde funkce vola spolecnou vnitrni funkci process_table
        logging.info("Funkce vola vnitrni funkci process_inside_table")
        log_list = process_inside_table(con_string_blob_storage, blob, loaded_json_config, json_config_single_table, stream, table_name, 
                                            extracted_headings)        
        return log_list
    
        # celkova chybna cesta, jmeno tabulky neni v json konfiguraci
    else:
        logging.info(f"Jmeno tabulky {table_name} nebylo rozpoznano v json konfiguraci")
        raise ValueError 

# zpracovava jednu tablku se zahlavnim nacitanym dynamicky, vystupem je slovnik s logem
def process_table_extract_headings_dynamically(con_string_blob_storage, blob, loaded_json_config):
    logging.info("Tabulka nebyla spravne rozpoznana dle json konfigurace, zkousim dalsi mozna nastaveni zahlavi...")

    # nacteni konfigurace
    json_config_all_tables = loaded_json_config[0]
    json_config_table_names = loaded_json_config[1]
    container_import = loaded_json_config[5]

    headings = pd.DataFrame
    log_list = list

     # stazeni vstupniho streamu
    stream = load_stream(con_string_blob_storage, container_import, blob)

    # nacteni tabulky zahlavi
    headings = load_headings(stream)
    table_name = str(headings.iloc[0, 0])

    # Pro dalsi zpracovani je nutne overit, zda je tabulka obsazena v json konfiguraci
    if table_name in json_config_table_names:
        logging.info(f"Tabulka {table_name} je v seznamu tabulek v json konfiguraci")

        # nacteni json konfigurace pro danou tabulku
        json_config_single_table = json_config_all_tables[table_name]
        logging.info(f"Tabulka {table_name} byla nactena")

        # nacteni informaci z tabulky zahlavi dznamicky
        extracted_headings = extract_headings_dynamically(table_name, json_config_single_table, headings)
        logging.info(f"Script uspesne overil, ze nactena pole tabulky {table_name} odpovidaji formatu typickemu pro dany typ informace")

        # zde funkce vola spolecnou vnitrni funkci process_table
        logging.info("Funkce vola vnitrni funkci process_inside_table")
        log_list = process_inside_table(con_string_blob_storage, blob, loaded_json_config, json_config_single_table, stream, table_name, 
                                            extracted_headings)
        return log_list
    
            # celkova chybna cesta, jmeno tabulky neni v json konfiguraci
    else:
        logging.info(f"Jmeno tabulky {table_name} nebylo rozpoznano v json konfiguraci")
        raise ValueError 

# vytvari chybovy log_list - unrecognised_file_log_list
def file_not_recognised_as_table_to_be_processed(blob):
    unrecognised_file_log_list = [{"Originalni jmeno": blob['name'], "Exportni jmeno": "", "Datum exportu":"",  "Kod davky": "", "Final": "", "Fis":"",
                "Fixace dat": "", "Fixace":"", "Dynamicky unpivot": "", "Kod datasetu":"", "Staticky unpivot":"",
                "Sloupce prejmenovany":"", "Radky sumace odstraneny":"", "Sloupec obdobi pridan": "",
                "Sloupec Kod datasetu vlozen": "", "Zpracovana": False, "Zahlavi nacteno":""}]
    return unrecognised_file_log_list


"""uroven 3"""

# vnitrni funkce process_table_extract_headings_by_json a process_table_extract_headings_dynamically

# stazeni a prevedeni souboru blob do bitove podoby
def load_stream(con_string_blob_storage, container_import, blob):    
    # blob client pripojeny na soubor v ulozisti blob
    blob_client = BlobClient.from_connection_string(
            con_string_blob_storage,
            container_name=container_import,
            blob_name=blob['name'],
        )
    
    # pozadavek na stazeni souboru
    logging.info("Script odeslal pozadavek na stazeni souboru:") 
    downloader = blob_client.download_blob()

    # object downloader musi byt preveden do bitove formy pro dalsi operace
    stream = downloader.content_as_bytes()
    logging.info("Soubor z blob storage stazen")
    return stream

# nacteni tabulky se zahlavim
def load_headings(stream):
    # headings nacitaji prvnich 12 sloupcu tabulky pro extrakci udaju v zahlavi
    # noinspection PyTypeChecker
    headings = pd.read_excel(stream, sheet_name="Sheet1", header=None, nrows=12, usecols=[0, 1])
    return headings

# ziskani informaci z tabulky ze zahlavi za pomoci nastaveni z jsonu
def extract_headings_by_json(table_name, json_config_single_table, headings):

    # z tabulky table info jsou nacteny informace nutne pro prevedeni zahlavi do metadat a setrizeni tabulek na surove tabulky s daty
    read_start = int(json_config_single_table["radek_zahlavi"])
    row_fixace_dat = int(json_config_single_table["fixace_dat"])
    row_beh = int(json_config_single_table["beh"])
    row_kriterium = int(json_config_single_table["kriterium"])

    # nacteni fixace dat z tabulky se zahlavim headings
    fixace_dat = headings.iloc[row_fixace_dat, 1]

    # spravne nacteni fixace dat je kontrolovano proti regexu
    regex_fixace_dat = re.compile(r"\w{2}\s\d{4}/\d{4}")
    if regex_fixace_dat.match(fixace_dat) is None:
        logging.info(f"Fixace dat tabulky {table_name} nebyla spravne nactena")
        raise ValueError
    else:
        logging.info(f"Fixace dat tabulky {table_name} je: {fixace_dat}")
    
    # z fixace dat je extrahovan semestr/y pro ktery/e je dana tabulky platna
    semestry = re.findall(regex_fixace_dat, fixace_dat)
    semestry_zahlavi = " - ".join(semestr for semestr in semestry)
    logging.info(f"Tabulka {table_name} platí pro semestr/y: {semestry_zahlavi}")

    # z fixace dat je extrahovano datum fixace, 
    try:
        regex_datum_fixace = re.compile(r"\d{2}.\s\d.\s\d{4}")
        fixace_list = re.findall(regex_datum_fixace, fixace_dat)
        datum_fixace = fixace_list[0]
        logging.info(f"Datum fixace dat tabulky {table_name} je: {datum_fixace}")
        if fixace_list == []:
            raise ValueError
    except:
        logging.info(f"Fixace tabulky {table_name} jako datum nebyla spravne nactena, jako fixace bude uzita cela fixace dat")
        datum_fixace = fixace_dat
    
    # nektere tabulky obsahuji beh a kriterium, ty jsou pridany do fixace_dat, a jejich spravnost je opet overena proti regexu
    if row_beh != 0:
        beh = headings.iloc[row_beh, 1]
        regex_3_letters = re.compile(r"\w{3}")
        if regex_3_letters.match(beh) is None:
            raise ValueError
        fixace_dat = fixace_dat + " - " + beh
    if row_kriterium != 0:
        kriterium = headings.iloc[row_kriterium, 1]
        regex_3_letters = re.compile(r"\w{3}")
        if regex_3_letters.match(kriterium) is None:
            raise ValueError
        fixace_dat = fixace_dat + " - " + kriterium

    return read_start, fixace_dat, semestry_zahlavi, datum_fixace

# zalozni funkce, ktera nacita zahlavi dynamicky, pokud selze nacitani z jsonu (v pripade minoritni zmeny ve formatu exportu z insisu)
def extract_headings_dynamically(table_name, headings):
    # z tabulky headings jsou nacteny informace nutne pro prevedeni zahlavi do metadat a setrizeni tabulek na surove tabulky s daty
    table_len = len(headings)
    for row_number in range(table_len):
        if headings.iloc[row_number, 0] in ["Období", "Dodavatel výkonu", "Typ výuky"]:
            read_start = row_number
        if headings.iloc[row_number, 0] == "Fixace dat":
            row_fixace_dat = row_number
        if headings.iloc[row_number, 0] == "Běh":
            row_beh = row_number
        if headings.iloc[row_number, 0] == "Kriterium započtení studentů":
            row_kriterium = row_number


    # nacteni fixace dat z tabulky se zahlavim headings
    fixace_dat = headings.iloc[row_fixace_dat, 1]

    # spravne nacteni fixace dat je kontrolovano proti regexu
    regex_fixace_dat = re.compile(r"\w{2}\s\d{4}/\d{4}")
    if regex_fixace_dat.match(fixace_dat) is None:
        logging.info(f"Fixace dat tabulky {table_name} nebyla spravne nactena")
        raise ValueError
    else:
        logging.info(f"Fixace dat tabulky {table_name} je: {fixace_dat}")
    
    # z fixace dat je extrahovan semestr/y pro ktery/e je dana tabulky platna
    semestry = re.findall(regex_fixace_dat, fixace_dat)
    semestry_zahlavi = " - ".join(semestr for semestr in semestry)
    logging.info(f"Tabulka {table_name} platí pro semestr/y: {semestry_zahlavi}")

    # z fixace dat je extrahovano datum fixace, 
    try:
        regex_datum_fixace = re.compile(r"\d{2}.\s\d.\s\d{4}")
        fixace_list = re.findall(regex_datum_fixace, fixace_dat)
        datum_fixace = fixace_list[0]
        logging.info(f"Datum fixace dat tabulky {table_name} je: {datum_fixace}")
        if fixace_list == []:
            raise ValueError
    except:
        logging.info(f"Fixace tabulky {table_name} jako datum nebyla spravne nactena, jako fixace bude uzita cela fixace dat")
        datum_fixace = fixace_dat
    
    # nektere tabulky obsahuji beh a kriterium, ty jsou pridany do fixace_dat, a jejich spravnost je opet overena proti regexu
    if row_beh != 0:
        beh = headings.iloc[row_beh, 1]
        regex_3_letters = re.compile(r"\w{3}")
        if regex_3_letters.match(beh) is None:
            raise ValueError
        fixace_dat = fixace_dat + " - " + beh
    if row_kriterium != 0:
        kriterium = headings.iloc[row_kriterium, 1]
        regex_3_letters = re.compile(r"\w{3}")
        if regex_3_letters.match(kriterium) is None:
            raise ValueError
        fixace_dat = fixace_dat + " - " + kriterium

    return read_start, fixace_dat, semestry_zahlavi, datum_fixace

# funkce ktera zastresuje generovani kodu davky a pripravuje pole pro generovani kodu datasetu, a rozdeluje tabulky podle formatu
def process_inside_table(con_string_blob_storage, blob, loaded_json_config, json_config_single_table, stream, table_name, 
                                             extracted_headings): 
    
    # generovani kodu davky kod davky, a prekodujici udaje z jsonu pro dalsi uziti pri generaci kodu datasetu
    generated_codes = generate_codes(loaded_json_config, table_name, extracted_headings)


    # zalozeni exportniho clientu blob service z connection string, kam budou nasledne ukladany vystupy
    blob_service_client = BlobServiceClient.from_connection_string(con_string_blob_storage) 

    # funkce rozdelujici tabulky podle formatu pro dalsi zpracovani
    log_list = process_inside_table_decide_format_type(blob_service_client, blob, loaded_json_config, json_config_single_table, stream, table_name, 
                                             extracted_headings, generated_codes)
    
    return log_list


"""uroven 4"""

# vnitrni funkce process_inside_table

# funkce generujici kod davky, a prekodujici udaje z jsonu pro dalsi 
# uziti pri generaci kodu datasetu ve funkcich add_code_delete_sum_rows a add_period_delete_sum_rows
def generate_codes(loaded_json_config, table_name, extracted_headings):

    semestry_zahlavi = extracted_headings[2]
    export_date = loaded_json_config[4]
    final_test_flag = loaded_json_config[2]
    fis_vse_flag = loaded_json_config[3]


    # funkce na vstupu prijima data o exportu z json konfigurace, ta jsou zde prevedena do spravneho formatu pro vytvoreni kodu davky
    if fis_vse_flag in ["ano", "Ano", "Yes", "yes", True, "FIS", "Fis"]:
        fis_code = "FIS"
    else:
        fis_code = "VSE"
    if final_test_flag in ["ano", "Ano", "Yes", "yes", True, "FIS", "Fis"]:
        flag_code = "FINAL"
    else:
        flag_code = "TEST"  

    # prevedeni data exportu z json konfigurace do podoby kodu 
    export_code = export_date.replace(".", "") 
    
    semestr_dose_code_stage_1 = "".join(semestry_zahlavi.split())
    semestr_dose_code_stage_2 = semestr_dose_code_stage_1.replace(",", "")
    semestr_dose_code_stage_3 = semestr_dose_code_stage_2.replace("-", "")
    semestr_dose_code = semestr_dose_code_stage_3.replace("/", "")
    

    # kod davky je nasledne slozen z kodu semestr, znacek fis_vse_flag/vse, final/test, a kodu casu exportu
    dose_code = semestr_dose_code + fis_code + export_code 
    logging.info(f"Pro tabulku {table_name} byl vygenerovan kod davky: {dose_code}")


    return flag_code, fis_code, export_code, dose_code

# funkce rozdelujici tabulky podle formatu, vola dalsi funkce nacitajici tabulky
def process_inside_table_decide_format_type(blob_service_client, blob, loaded_json_config, json_config_single_table, stream, table_name, 
                                             extracted_headings, generated_codes):
    
    # "klasicka" cesta, pro vsechny tabulky se "standardnim formatem"
    if json_config_single_table["specialni_format"] == "ne":
        logging.info(f"Tabulka {table_name} nema specialni format") 

        log_list = process_inside_table_conventional(blob, blob_service_client, stream, json_config_single_table, loaded_json_config,
                                                        table_name, extracted_headings, generated_codes)
    
    # tabulky ktere se prilis vymikaji ve svem formatu "standardu" prochazi pres specialni cestu
    # jmenovite jsou to "parametry vypoctu" a "prehled spojene vyuky"
    elif json_config_single_table["specialni_format"] == "ano":
        logging.info(f"Tabulka {table_name} ma specialni format") 

        # tabulka prehled spojene vyuky ma zvlastni formatovani zahlavi
        if table_name == "Přehled spojené výuky":
            log_list = process_inside_table_prehled_spojene_vyuky(blob, blob_service_client, stream, table_name, json_config_single_table,
                                extracted_headings, loaded_json_config, generated_codes)

        # tabulka se zcela vymyka formatu, do azure blob storage se nahrava bitova kopie puvodniho soboru stazena na zacatku procedury
        elif table_name == "Parametry výpočtu":   
                    log_list = process_inside_table_parametry_vypoctu(blob, blob_service_client, stream, table_name, 
                                            extracted_headings, loaded_json_config, generated_codes)
        else:
            logging.info(f"Tabulka {table_name} nema specifikovany typ specialnho formatu")
            raise ValueError
    else:
        logging.info(f"Tabulka {table_name} nema specifikovanou informaci o specialnim formatu") 
        raise ValueError
    
    return log_list

"""uroven 5"""

# vnitrni funkce process_inside_table_decide_format_type

# klasicka cesta pro klasickou tabulku
def process_inside_table_conventional(blob, blob_service_client, stream, json_config_single_table, loaded_json_config,
                                    table_name, extracted_headings, generated_codes):
    # nacteni konfigurace
    read_start = extracted_headings[0]
    container_export = loaded_json_config[6]
     
    # promenne
    table = pd.DataFrame

    # nacteni tabulky
    table = load_table(stream, read_start, table_name)

    # zprocesovani nahrane tabulky
    table, dict_of_codes_in_table, result = modify_table(json_config_single_table, table, table_name, 
                                                                           extracted_headings, generated_codes)

    # export tabulky
    name_of_export = export_table(table, blob_service_client, container_export, table_name)

    # tvorba souhrnneho logu jednotlive tabulky
    log_list = create_log_table(blob, loaded_json_config, extracted_headings, generated_codes,
                                         name_of_export, dict_of_codes_in_table, table_name, result)
    
    logging.info(f"Tabulka {table_name} byla uspesne zpracovana standartni cestou")

    return log_list

# specialni cesta pro tabulku prehled spojene vyuky
def process_inside_table_prehled_spojene_vyuky(blob, blob_service_client, stream, table_name, json_config_single_table,
                                extracted_headings, loaded_json_config, generated_codes):
    #promenne
    table = pd.DataFrame

    # nacteni konfigurace
    read_start = extracted_headings[0]
    container_export = loaded_json_config[6]

    # nacteni tabulky
    table = load_table_prehled_spojene_vyuky(stream, read_start, table_name)

    # provedeni operaci specifickych pro tabulku
    table, dict_of_codes_in_table, result = modify_table_prehled_spojene_vyuky(json_config_single_table, table, table_name, generated_codes)
    
    # export tabulky standartni funkci
    name_of_export = export_table(table, blob_service_client, container_export, table_name)

    # tvorba souhrnneho logu jednotlive tabulk
    log_list = create_log_table(blob, loaded_json_config, extracted_headings, generated_codes,
                        name_of_export, dict_of_codes_in_table, table_name, result)
    
    logging.info(f"Tabulka {table_name} byla uspesne zpracovana cestou prehled spojene vyuky")

    return log_list

# specialni cesta pro tabulku parametry vypoctu 
def process_inside_table_parametry_vypoctu(blob, blob_service_client, stream, table_name, 
                                            extracted_headings, loaded_json_config, generated_codes):

    # export bitove kopie puvodniho souboru parametru vypoctu
    name_of_export = export_table_parametry_vypoctu(blob_service_client, stream, table_name, loaded_json_config)

    # tvorba souhrneho logu pro tabulku parametry vypoctu
    log_list = create_log_table_parametry_vypoctu(blob, table_name, extracted_headings, loaded_json_config, 
                                            generated_codes, name_of_export)
    
    logging.info(f"Tabulka {table_name} byla uspesne zpracovana cestou parametru vypoctu")

    return log_list


"""uroven 6"""


# vnitrni funkce process_inside_table_conventional, process_inside_table_prehled_spojene_vyuky, process_inside_table_parametry_vypoctu

# nacteni samotne tabulky do pandas dataframe pro tabulky zpracovane klasickou cestou
def load_table(stream, read_start, table_name):
    # samotne nacteni
    table = pd.read_excel(stream, sheet_name="Sheet1", skiprows=read_start)
    logging.info(f"Tabulka {table_name} byla uspesne nactena do pandas")
    return table

# klasicka cesta uprav pandas Dataframe 
def modify_table(json_config_single_table, table, table_name, extracted_headings, generated_codes):

    logging.info(f"Tabulka {table_name} bude nyni upravena...")
    # nacteni konfigurace
    semestry_zahlavi = extracted_headings[2]
    flag_code = generated_codes[0]
    fis_code = generated_codes[1]
    export_code = generated_codes[2]
    dose_code = generated_codes[3]

    # promenne
    rotated_dynamically = False
    rotated_statically = False
    columns_renamed = False
    sumace_deleted = False
    column_obdobi_added = False
    code_inserted = False

    # nacteni nastaveni pro danou tabulku z konfigurace
    delete_columns_sumation_flag = json_config_single_table["odstraneni_sloupcu_sumace"]
    unpivot_dynamicky_flag = json_config_single_table["unpivot_dynamicky"]
    unpivot_staticky_flag = json_config_single_table["unpivot_staticky"]
    add_column_period_flag = json_config_single_table["pridani_sloupce_obdobi"]
    logging.info(f"Nastaveni pro tabulku {table_name} z jsonu bylo nacteno")

    # tabulky urcene pro odstraneni sloupcu sumace
    if delete_columns_sumation_flag == "ano":
        logging.info(f"Tabulka {table_name} je urcena pro odstraneni sloupcu sumace")
        table = delete_sumation_columns(json_config_single_table, table, table_name)
    elif delete_columns_sumation_flag == "ne":
            logging.info(f"Tabulka {table_name} neni urcena pro odstraneni sloupcu sumace")
    else:
        logging.info(f"Tabulka {table_name} nema specifikovane urceni pro odstraneni sloupcu sumace")
        raise ValueError
    
    # tabulky urcene pro dynamicky unpivot
    if unpivot_dynamicky_flag == "ano":
        logging.info(f"Tabulka {table_name} je urcena pro dynamicky unpivot")
        table = dynamic_unpivot(json_config_single_table, table, table_name)
        rotated_dynamically = True

    # tabulky u nichz dynamicky unpivot neprovadime
    elif unpivot_dynamicky_flag == "ne":
        logging.info(f"Tabulka {table_name} neni urcena pro dynamicky unpivot")

    # chybova cesta pokud script prostrada informaci o dynamickem unpivotu v json konfiguraci
    else:
        logging.info(f"Tabulka {table_name} nema specifikovane urceni pro operaci unpivot")
        raise ValueError
    
    # nasledne je v dalsim kroku proveden u vybranych tabulek unpivot staticky
    if unpivot_staticky_flag == "ano":
        logging.info(f"Tabulka {table_name} je urcena pro staticky unpivot")
        table = static_unpivot(json_config_single_table, table, table_name)
        columns_renamed = True
        rotated_statically = True

    # tabulky u nichz unpivot neprovadime jsou opet pouze zkoprivany do vystupniho souboru
    elif unpivot_staticky_flag == "ne":
        logging.info(f"Tabulka {table_name} neni urcena pro staticky unpivot")

    # chybova cesta pokud script prostrada informaci o statickem unpivotu v json konfiguraci
    else:
        logging.info(f"Tabulka {table_name} nema specifikovane urceni pro operaci staticky unpivot")
        raise ValueError(f"Tabulka {table_name} nema specifikovane urceni pro operaci staticky unpivot")

    # do tabulek bez sloupce "Obdobi" je tento pridan ze zahlavi, jedna se o tabulky bilanci    
    if add_column_period_flag == "ano":
        logging.info(f"Tabulka {table_name} je urcena pro vlozeni sloupce Obdobi")
        table, dict_of_codes_in_table = add_period_delete_sum_rows(json_config_single_table, table, table_name, dose_code, semestry_zahlavi)
        sumace_deleted = True
        code_inserted = True
        column_obdobi_added = True

    elif add_column_period_flag == "ne":
        logging.info(f"Tabulka {table_name} neni urcena pro vlozeni sloupce Obdobi")
        table, dict_of_codes_in_table = add_code_delete_sum_rows(json_config_single_table, table, table_name, fis_code, export_code, flag_code)
        sumace_deleted = True
        code_inserted = True

    else:
        logging.info(f"Tabulka {table_name} nema specifikovane urceni pro operaci vlozeni sloupce obdobi")
        raise ValueError(f"Tabulka {table_name} nema specifikovane urceni pro operaci vlozeni sloupce obdobi")   

    result = [rotated_dynamically, rotated_statically, columns_renamed, 
            sumace_deleted,  column_obdobi_added, code_inserted, column_obdobi_added, code_inserted]
    
    return table, dict_of_codes_in_table, result

# exportni funkce
def export_table(table, blob_service_client, container_export, table_name):

    # tvorba jmena exportu
    name_of_export = generate_export_name(table_name)

    # vytvoreni exportniho blob clienta pro samotny soubor 
    blob_client_export = blob_service_client.get_blob_client(container=container_export, blob=name_of_export)
    logging.info(f"Byl vytvoren exportni client azure blob service pro tabulku {table_name}")  

    # export do blob storage
    export_virtual_excel_file(table, table_name, name_of_export, blob_client_export)

    return name_of_export

# tvorba dilcich metadat
def create_log_table(blob, loaded_json_config, extracted_headings, generated_codes,
                        name_of_export, dict_of_codes_in_table, table_name, result):
    
    # nacteni konfigurace
    fixace_dat = extracted_headings[1]
    datum_fixace = extracted_headings[3]
    export_date = loaded_json_config[4]
    flag_code = generated_codes[0]
    fis_code = generated_codes[1]
    dose_code = generated_codes[3]

    # nacteni seznamu provadenych operaci
    rotated_dynamically = result[0]
    rotated_statically  = result[1]
    columns_renamed = result[2]
    sumace_deleted = result[3]
    column_obdobi_added = result[4]
    code_inserted = result[5]
    column_obdobi_added = result[6]
    code_inserted = result[7]

    # sezanm pro ukladani logu pro jednotlive kody datasetu
    log_list = []

    # nakonec se generuje slovnik s informacemi o tabulce a provadenych operaci pro kazdy dany kod datasetu
    for code in dict_of_codes_in_table:
        log_dict = {"Kod datasetu": code, "Originalni jmeno": blob['name'], "Exportni jmeno": name_of_export, "Datum exportu":export_date, "Semestr": dict_of_codes_in_table[code], "Final": flag_code, "Fis": fis_code,
                        "Fixace dat": fixace_dat, "Kod davky": dose_code, "Fixace":datum_fixace, "Dynamicky unpivot": rotated_dynamically,"Staticky unpivot":rotated_statically,
                        "Sloupce prejmenovany":columns_renamed, "Radky sumace odstraneny":sumace_deleted, "Sloupec obdobi pridan": column_obdobi_added,
                        "Sloupec Kod datasetu vlozen": code_inserted, "Zpracovana": True}
        
        log_list.append(log_dict)

    logging.info(f"Byla vytvorena dilci cast logu pro tabulku {table_name}")

    return log_list

# nacteni samotne tabulky; cesta prehled spojene vyuky
def load_table_prehled_spojene_vyuky(stream, read_start, table_name):
    # samotne nacteni
    table = pd.read_excel(stream, sheet_name="Sheet1", skiprows=read_start, header=[0,1])
    logging.info(f"Tabulka {table_name} byla uspesne nactena")
    return table

# specialni cesta uprav; cesta prehled spojene vyuky
def modify_table_prehled_spojene_vyuky(json_config_single_table, table, table_name, generated_codes):
    
    logging.info(f"Tabulka {table_name} bude nyni upravena...")
    #nactenni konfigurace
    flag_code = generated_codes[0]
    fis_code = generated_codes[1]
    export_code = generated_codes[2]

    # promenne
    rotated_dynamically = False
    rotated_statically = False
    columns_renamed = False
    sumace_deleted = False
    column_obdobi_added = False
    code_inserted = False
    
    # odstraneni prvni urovne zahlavi
    table = drop_first_level_headers(table)

    # prejmenovani sloupcu
    table = rename_columns_from_list_of_column_names(json_config_single_table, table)
    columns_renamed = True

    # pridani kodu datasetu a odstraneni radku se sumaci
    table, dict_of_codes_in_table = add_code_delete_sum_rows(json_config_single_table, table, table_name, fis_code, export_code, flag_code)
    code_inserted = True
    sumace_deleted = True

    result = [rotated_dynamically, rotated_statically, columns_renamed, 
            sumace_deleted,  column_obdobi_added, code_inserted, column_obdobi_added, code_inserted]
    
    logging.info(f"Tabulka {table_name} byla uspesne upravena")
    
    return table, dict_of_codes_in_table, result

# exportni funkce; cesta parametry vypoctu
def export_table_parametry_vypoctu(blob_service_client, stream, table_name, loaded_json_config):
    
    # nacteni konfigurace
    container_export = loaded_json_config[6]

    # tvorba jmena exportu
    name_of_export = generate_export_name(table_name)
    
    # export bitove kopie souboru
    export_bite_copy(blob_service_client, stream, table_name, container_export, name_of_export)

    return name_of_export

# tvorba dilcich metadata; cesta parametry vypoctu
def create_log_table_parametry_vypoctu(blob, table_name, extracted_headings, loaded_json_config, 
                                            generated_codes, name_of_export):
    # nacteni konfigurace
    fixace_dat = extracted_headings[1]
    semestry_zahlavi = extracted_headings[2]
    datum_fixace = extracted_headings[3]
    export_date = loaded_json_config[4]
    flag_code = generated_codes[0]
    fis_code = generated_codes[1]
    dose_code = generated_codes[3]

    # promenne
    rotated_dynamically = False
    rotated_statically = False
    columns_renamed = False
    sumace_deleted = False
    column_obdobi_added = False
    code_inserted = False

    # nakonec se generuje slovnik s informacemi o tabulce a provadenych operacic
    log_list = [{"Originalni jmeno": blob['name'], "Exportni jmeno": name_of_export, "Datum exportu":export_date, "Kod davky": dose_code, "Final": flag_code, "Fis":fis_code,
                "Fixace dat": fixace_dat, "Semestr":semestry_zahlavi, "Fixace":datum_fixace, "Dynamicky unpivot": rotated_dynamically, "Kod datasetu":dose_code, "Staticky unpivot":rotated_statically,
                "Sloupce prejmenovany":columns_renamed, "Radky sumace odstraneny":sumace_deleted, "Sloupec obdobi pridan": column_obdobi_added,
                "Sloupec Kod datasetu vlozen": code_inserted, "Zpracovana": True}]

    logging.info(f"Byla vytvorena dilci cast metadat pro danou tabulku {table_name}")

    return log_list


"""uroven 7"""

# vnitrni funkce modify_table, modify_table_prehled_spojene_vyuky, export_table a export_table_parametry_vypoctu
# pomocne funkce

# odstraneni sloupcu se soucty
def delete_sumation_columns(json_config_single_table, table, table_name):
    
    # nacteni konfigurace
    columns_sumation = json_config_single_table["sloupce_sumace"]

    # seznam odstranenych sloupcu
    columns_deleted_list = []
    
    # odstraneni sloupcu se soucty
    for column_name in columns_sumation:
        for column in table.columns:
            if column_name == column:
                del table[column]
                columns_deleted_list.append(column_name)

    columns_deleted = ", ".join(column for column in columns_deleted_list)
    logging.info(f"Z tabulky {table_name} byly smazany sloupce {columns_deleted}")

    return table

# dynamicky unpivot
def dynamic_unpivot(json_config_single_table, table, table_name):

    # z json konfigurace jsou nacitany specifikace operace
    var_name_dynamically = json_config_single_table["sloupec_atributu_dynamicky"]
    value_name_dynamically = json_config_single_table["sloupec_hodnoty_dynamicky"]
    fixed_columns_dynamically = json_config_single_table["fixni_sloupce_dynamicky"]

    # operace dynamicky unpivot
    table = pd.melt(table, id_vars=fixed_columns_dynamically, var_name=var_name_dynamically, value_name=value_name_dynamically)

    logging.info(f"Tabulka {table_name} byla upravena dynamicky pomoci funkce unpivot")
    return table

# staticky unpivot
def static_unpivot(json_config_single_table, table, table_name):

    # nacteni konfigurace
    var_name_staticky = json_config_single_table["sloupec_atributu_staticky"]
    value_name_staticky = json_config_single_table["sloupec_hodnoty_staticky"]
    fixed_columns_staticky = json_config_single_table["fixni_sloupce_staticky"]
    change_columns = json_config_single_table["nahrazeni_jmen_sloupcu"]

    # u tabulek je nutne take provest prejmenovani sloupcu pred unpivotem, abychom meli ve vyslednych tabulkach pouze kody
    if change_columns == "ano":
        columns_to_change = json_config_single_table["slovnik_pro_nahrazeni"]
        logging.info(f"Tabulka {table_name} je urcena pro prejmenovani sloupcu ")
        table.rename(columns=columns_to_change, inplace=True)
        logging.info(f"Tabulka {table_name} byla uspesne prejmenovana")

    # operace statickeho unpivotu
    table = table.melt(id_vars=fixed_columns_staticky, var_name=var_name_staticky, value_name=value_name_staticky)
    logging.info(f"Tabulka {table_name} byla uspesne upravena pomoci statickeho unpivotu")

    return table

# proctenni tabulky a pridani kodu datasetu a odstraneni radku se soucty
# tyto dve funkce jsou spojeny dohromady pro vypocetni slozitost procitanni celeho dataframu pomoci iterrrows():
def add_code_delete_sum_rows(json_config_single_table, table, table_name, fis_code, export_code, flag_code):

    # slovnik kodu datasetu v tabulce
    dict_of_codes_in_table = {}

    # pocitadlo smazanych radku
    counter_for_drop = 0

    # znacky indikujici provadene operace
    sumace_flag = False
    skip_row_flag = False

    # nacteni znacky z json konfigurace
    if json_config_single_table["odstraneni_radku_sumace"] == "ano":       
        sloupec_sumace = json_config_single_table["sloupec_sumace"]   
        sumace_flag = True  

    # finalni upravy exportnich tabulek a vlozeni kodu datasetu
    for index, row in table.iterrows():
        skip_row_flag = False

        # nektere tabulky obsahuji prubezne radky se soucty, tyto musi byt z tabulek odstraneny
        if sumace_flag == True:       
            if row[sloupec_sumace] == "Suma":
                table.drop(labels=[index], inplace=True)
                counter_for_drop += 1
                skip_row_flag = True
            if row[sloupec_sumace] == "Celkem":
                table.drop(labels=[index], inplace=True)
                counter_for_drop += 1
                skip_row_flag = True

        # pro smazane radky se kod datasetu negeneruje
        if skip_row_flag == False:
            period = row["Období"]
            regex_fixace_dat = re.compile(r"\w{2}\s\d{4}/\d{4}")
            semestry = re.findall(regex_fixace_dat, period)
            semestr = "- ".join(semestr for semestr in semestry)

            # hodnota semestr je prevedena do podoby kodu
            semestr_code_stage_1 = "".join(semestr.split())
            semestr_code_stage_2 = semestr_code_stage_1.replace(",", "")
            semestr_code_stage_3 = semestr_code_stage_2.replace("-", "")
            semestr_code = semestr_code_stage_3.replace("/", "")

            # kod datasetu je nasledne slozen z kodu semestr, znacek fis_vse_flag/vse, final/test, a kodu casu exportu
            code = semestr_code + fis_code + export_code 

            # unikatni kod je spolecne se svym semestrem zaznamenan
            if code not in dict_of_codes_in_table:
                dict_of_codes_in_table.update({code:semestr})

            # do tabulky je vlozen sloupec s kodem datasetu
            table.loc[index, "Kód Datasetu"] = code
            
    if counter_for_drop != 0:
        logging.info(f"Z tabulky {table_name} bylo odstraneno {counter_for_drop} radku s prubeznou sumaci")

    return table, dict_of_codes_in_table

# proctenni tabulky a pridani sloupce obdobi, sloupce kodu datasetu a odstraneni radku se soucty
# tyto dve funkce jsou spojeny dohromady pro vypocetni slozitost procitanni celeho dataframu pomoci iterrrows():
def add_period_delete_sum_rows(json_config_single_table, table, table_name, dose_code, semestry_zahlavi):

    # do tabulky je vlozen sloupec obdobi ze zahlavi
    table["Období"] = semestry_zahlavi

    # do tabulky je jako kod datasetu vkladan kod davky, v tomto pripade jsou totozne
    table["Kód datasetu"] = dose_code

    # slovnik kodu datasetu v tabulce
    dict_of_codes_in_table = {}

    # pocitadlo smazanych radku
    counter_for_drop = 0

    # tyto tabulky obsahuji prubezne radky se soucty tyto musi byt z tabulek odstraneny  
    if json_config_single_table["odstraneni_radku_sumace"] == "ano":  

        sloupec_sumace = json_config_single_table["sloupec_sumace"]  

        for index, row in table.iterrows(): 
            if row[sloupec_sumace] == "Suma":
                table.drop(labels=[index], inplace=True)
                counter_for_drop += 1
            if row[sloupec_sumace] == "Celkem":
                table.drop(labels=[index], inplace=True)
                counter_for_drop += 1

    if counter_for_drop != 0:
        logging.info(f"Z tabulky {table_name} bylo odstraneno {counter_for_drop} radku s prubeznou sumaci")

    # unikatni kod je spolecne se svym semestrem zaznamenan
    if dose_code not in dict_of_codes_in_table:
        dict_of_codes_in_table.update({dose_code:semestry_zahlavi})

    return table, dict_of_codes_in_table

# funkce na generovani jmena exportu
def generate_export_name(table_name):

    # pojmnenovani tabulky na zaklade jmena tabulky v headings
    name_of_export = table_name + ".xlsx"
    logging.info(f"Tabulka {table_name} bude exportovana jako {name_of_export}")  
    return name_of_export

# funkce na export bitove kopie excelu vytvoreneho z pandas dataframe nacteneho v predchozich krocich
def export_virtual_excel_file(table, table_name, name_of_export, blob_client_export):

    # exportni procedura vytvari virtualni excel soubor pomoci io.bytesio a pandas.to_excel funkce
    logging.info(f"Vytvorena exportni tabulka {table_name} bude exportovana do azure blob storage")   
    filename= io.BytesIO()
    with pd.ExcelWriter(filename, engine = "xlsxwriter") as writer:
        table.to_excel(writer, sheet_name = "Sheet1", index = False)

    # z ktere pote ziskame bitovy obraz
    xlsx_data = filename.getvalue()
    logging.info(f"Script odeslal pozadavek na zapsani souboru: {name_of_export}") 

    # a ten je nahravan do pripraveneho exportniho blobu
    blob_client_export.upload_blob(xlsx_data, blob_type = "BlockBlob", overwrite=True)
    logging.info(name_of_export + " byla exportovana!") 

# odstraneni prvni urovne zahlavi surove tabulky; cesta prehled spojene vyuky
def drop_first_level_headers(table):
    # odstraneni prvni urovne zahlavi
    table = table.droplevel(0, axis=1)
    return table

# prejmenovani jmen sloupcu podle seznamu v listu; cesta prehled spojene vyuky
# tato funkce je potencialne problematicka pri jakkekoli zmene poradi sloupcu etc.
# bohuzel vsak musi byt provedena takto, protoze se nektere sloupce jmenuji stejne
def rename_columns_from_list_of_column_names(json_config_single_table, table):
    # uprava spatne nacteneho duplicitniho prvniho sloupce "obdobi" a prejmenovani ostatnich sloupcu podle prvni urovne zahlavi
    columns_spojena_vyuka = json_config_single_table["slovnik_pro_nahrazeni"]
    table.columns = columns_spojena_vyuka
    return table

# exportuje pouze stazennou bitovou kopii souboru bez nacteni; cesta parametry vypoctu
def export_bite_copy(blob_service_client, stream, table_name, container_export, name_of_export):

    #vytvoreni exportniho blob clienta pro samotny soubor 
    blob_client_export = blob_service_client.get_blob_client(container=container_export, blob=name_of_export)
    logging.info(f"Byl vytvoren exportni client azure blob service pro tabulku {table_name}")  

    # bitova kopie se nahrava pomoci io.bytesio
    filename= io.BytesIO()
    filename = stream

    # a ten je nahravan do pripraveneho exportniho blobu
    logging.info(f"Script odeslal pozadavek na zapsani souboru pro tabulku {table_name}:") 
    blob_client_export.upload_blob(filename, blob_type = "BlockBlob", overwrite=True)
    logging.info(name_of_export + " byla exportovana!")


# samotna funkce
app = func.FunctionApp()
@app.function_name(name="unpivot-prestage-projekt-vykony")
@app.route(route="unpivot-prestage")
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function unpivot-prestage-projekt-vykony byla spustena...")
    # hlavni funkce ktera obsahuje 5 hlavni dilci funkce a řadu jejich vnitrnich funkci, 
    # sama o sobe orchestruje cely proces nacteni souboru ze vstupniho blobu, jejich upravy a nasledneho nahrani do blobu vystupniho,
    # nejprve nahrava json konfiguraci, pote nacita seznam souboru v blob ulozsti (load_json_configuration, list_all_files_in_blob),
    # a nasledne (process_all_files_in_blob):
    # 1. zkousi cestu ktera nacita tabulky podle konfigurace v jsonu (process_table_extract_headings_by_json)
    # 2. pokud tato selze zkousi cestu s dynamickym nacitanim zahlavi tabulek (process_table_extract_headings_dynamically)
    # 3. pokud selze i tato konstatuje, ze soubor nebyl rozpoznan a nemuze byt nacten (file_not_recognised_as_table_to_be_processed)
    # nasledne je v dalsi vrstve rozhodnuto dle json konfigurace, zda je tabulka urcena pro zpracovani konvencni a nebo specialni (process_inside_table),
    # a dochazi k samotnemu zpracovani tabulky (process_inside_table_conventional...)
    # nejprve je cela tabulka nactena do pandas, pote jsou na ni provedeny upravy specifikovane v kofiguraci (vyjma parametru vypoctu) (load_table, modify_table),
    # pote je exportovan bitovy obraz virtualni generovaneho excelu na zaklade pandas dataframu (export_table),
    # dalsi krok je generace dilciho logu a zaznamenami provedene operace (create_log_table)
    # a poslednim krokem je smazani vsech souboru ve vstupnim ulozisti blob po uspesnem zpracovani (delete_all_files_in_blob)


    # create_metadata:
    # 1. nacita vystupy predeslych funkci a  informace o prubehu procedury (create_log)
    # 2. vytvari log operace z vystupu predeslych funkci  (create_log)
    # 3. nasledne shrnuje informace o exportu do souboru s metadaty (create_metadata_file)
    # 4. vytvari zaverecnou zpravu shrnujici prubeh operace (create_final_message)


    # connection string na blob storage
    con_string_blob_storage = ""

    # nacteni json konfigurace
    loaded_json_config = load_json_configuration(con_string_blob_storage)

    # procteni blob storage 
    list_of_all_files_in_blob = list_all_files_in_blob(con_string_blob_storage, loaded_json_config)

    # aplikovani dilcich funkci na pritomne soubory
    processed_files = process_all_files_in_blob(con_string_blob_storage, loaded_json_config, list_of_all_files_in_blob)
                                                
    # tvorba logu a metadat, shrnuti prubehu funkce do zaverecne zpravy
    message, fail_flag = create_metadata(con_string_blob_storage, loaded_json_config, processed_files)

    # pokud probehlo spravne nacteni a zpracovani vsech souboru, a je tak specifikovano v json konfiguraci,
    # jsou vsechny vstupni soubory smazany pro usnadneni rychleho nahrani dalsi davky 
    message = delete_all_files_in_blob(con_string_blob_storage, loaded_json_config, list_of_all_files_in_blob, fail_flag, message)

    # odeslani vypsane zpravy o celkovem pruberu do api
    return func.HttpResponse(message)
    

    

