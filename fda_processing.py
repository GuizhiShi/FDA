__author__ = 'mq20155490'

import os, zipfile, MySQLdb

import urllib2

db = MySQLdb.connect(host='bh3.duckdns.org',user='fda_user',passwd='fda',db='fda',local_infile=1)

def fda_data_processing():
    file_names = ["mdrfoi","patient","foidev","foitext"]
    file_names = ["foitext"]
    zip_file_url = "http://www.accessdata.fda.gov/MAUDE/ftparea/"
    for file_name in file_names:

        add_file_name = file_name+"add.zip"
        change_file_name = file_name+"change.zip"

        add_file_fullurl = zip_file_url + add_file_name

        check_file_is_exists("zip/"+add_file_name)

        download_and_archive(add_file_fullurl,add_file_name)


        load_data_into_db(file_name,"Add")


        change_file_fullurl = zip_file_url + change_file_name

        check_file_is_exists("zip/"+change_file_name)

        download_and_archive(change_file_fullurl,change_file_name)

        load_data_into_db(file_name,"Change")





def check_file_is_exists(file_name):

    #downdload text table zip file
    file_path = os.path.dirname(os.path.realpath(__file__))
    #check zip file

    zip_is_exists = os.path.exists(file_path+"/"+file_name)
    if zip_is_exists:
        os.remove(file_path+"/"+file_name)


def download_and_archive(file_fullurl,add_file_name):


    f = urllib2.urlopen(file_fullurl)
    data = f.read()
    with open("zip/"+add_file_name, "wb") as code:
        code.write(data)

    #zf = zipfile.ZipFile('foitextchange.zip', 'w')
    txt_file_name = add_file_name.replace(".zip",".txt")
    check_file_is_exists("txt/"+txt_file_name)
    with zipfile.ZipFile("zip/"+add_file_name) as zf:
            zf.extractall("txt")



def load_data_into_db(file_name,update_type):

    cursor = db.cursor()
    file_path = os.path.dirname(os.path.realpath(__file__))+"/txt/"+file_name+update_type+".txt"
    if file_name == 'mdrfoi':

        #file_path = "/txt/"+file_name+update_type+".txt"
        load_sql = """
        LOAD DATA LOCAL INFILE '{0}'
        INTO TABLE {1}
        FIELDS TERMINATED BY '|' IGNORE 1 LINES
        ( @mdr_report_key, @event_key, report_number, report_source_code, manufacturer_link_flag,
        	number_devices_in_event, number_patients_in_event, @daterec, adverse_event_flag,
        	product_problem_flag, @daterep, @dateeve, single_use_flag_reprocessor_flag,
        	reporter_occupation_code, health_professional, initial_report_to_FDA,
        	@datefac,  @daterepp, report_to_fda, @datefda, event_location,
        	@dateman,manufacturer_contact_title, manufacturer_contact_first_name, manufacturer_contact_last_name,
        	manufacturer_contact_street_1, manufacturer_contact_street_2, manufacturer_contact_city, manufacturer_contact_state_code,
        	manufacturer_contact_zip_code, manufacturer_contact_zip_code_ext, manufacturer_contact_country_code,
        	manufacturer_contact_postal_code,manufacturer_contact_phone_no_area_code, manufacturer_contact_phone_no_exchange,
        	manufacturer_contact_phone_no, manufacturer_contact_phone_no_ext, manufacturer_contact_phone_no_country_code,
        	manufacturer_contact_phone_no_city_code, manufacturer_contact_phone_no_local, manufacturer_g1_name,
        	manufacturer_g1_street_1, manufacturer_g1_street_2, manufacturer_g1_city, manufacturer_g1_state_code,
        	manufacturer_g1_zip_code, manufacturer_g1_zip_code_ext, manufacturer_g1_country_code, manufacturer_g1_postal_code,
            @datemanrec,@datedevman, single_use_flag, remedial_action, previous_use_code, removal_correction_number, event_type,
            distributor_name, distributor_address_1, distributor_address_2, distributor_city,
        	distributor_state_code, distributor_zip_code, distributor_zip_code_ext,report_to_manufacturer,
        	manufacturer_name, manufacturer_address_1, manufacturer_address_2, manufacturer_city,
        	manufacturer_state_code, manufacturer_zip_code, manufacturer_zip_code_ext, manufacturer_country_code,
        	manufacturer_postal_code,type_of_report,source_type)
        SET
            mdr_report_key = CONVERT(@mdr_report_key,UNSIGNED INTEGER),
            event_key = CONVERT(@event_key,UNSIGNED INTEGER),
        	date_received = STR_TO_DATE(@daterec, '%m/%d/%Y'),
        	date_report = STR_TO_DATE(@daterep, '%m/%d/%Y'),
        	date_reported_to_fda = STR_TO_DATE(@datefda, '%m/%d/%Y'),
        	date_of_event = STR_TO_DATE(@dateeve, '%m/%d/%Y'),
        	date_facility_aware = STR_TO_DATE(@datefac, '%m/%d/%Y'),
        	report_date = STR_TO_DATE(@daterepp, '%m/%d/%Y'),
        	date_report_to_manufacturer = STR_TO_DATE(@dateman, '%m/%d/%Y'),
        	date_manufacturer_received = STR_TO_DATE(@datemanrec, '%m/%d/%Y'),
        	device_date_of_manufacture = STR_TO_DATE(@datedevman, '%m/%d/%Y');
                """.format(file_path,"raw_mdr_"+update_type.lower())


        delete_sql = """delete t.* from {0} t""".format("raw_mdr_"+update_type.lower())

        cursor.execute(delete_sql)
        db.commit()


        cursor.execute(load_sql)
        db.commit()

    if file_name == "patient":
        load_sql = """
        LOAD DATA LOCAL INFILE '{0}'
        INTO TABLE {1}
        FIELDS TERMINATED BY '|' IGNORE 1 LINES
        (mdr_report_key, patient_sequence_number, @daterec, sequence_number_treatment, sequence_number_outcome)
        SET date_received = STR_TO_DATE(@daterec, '%m/%d/%Y');
        """.format(file_path,"raw_patient_"+update_type.lower())

        delete_sql = """delete t.* from {0} t""".format("raw_patient_"+update_type.lower())

        cursor.execute(delete_sql)
        db.commit()


        cursor.execute(load_sql)
        db.commit()


    if file_name == "foidev":
        load_sql = """
        LOAD DATA LOCAL INFILE '{0}'
        INTO TABLE {1}
        FIELDS TERMINATED BY '|' IGNORE 1 LINES
        ( mdr_report_key, device_event_key, implant_flag, date_removed_flag,
        	device_sequence_no, @daterec, brand_name, generic_name, manufacturer_name,
        	manufacturer_address_1,	manufacturer_address_2, manufacturer_city,
        	manufacturer_state_code, manufacturer_zip_code, manufacturer_zip_code_ext,
        	manufacturer_country_code, manufacturer_postal_code, @dateexp,
        	model_number, lot_number, catalog_number, other_id_number, device_operator,
        	device_availability, @dateret, device_report_product_code,
        	device_age, device_evaluated_by_manufacturer, baseline_brand_name,
        	baseline_generic_name, baseline_model_no, baseline_catalog_no, baseline_other_id_no,
        	baseline_device_family, baseline_shelf_life_in_label, baseline_shelf_life_in_months,
        	baseline_PMA_flag, baseline_PMA_no, baseline_510k_flag, baseline_510k_no,
        	baseline_preamendment, baseline_transitional,baseline_510k_exempt_flag,
        	@datefir,@datecea)
        SET
        	date_received = STR_TO_DATE(@daterec, '%m/%d/%Y'),
        	expiration_date_of_device = STR_TO_DATE(@dateexp, '%m/%d/%Y'),
        	date_returned_to_manufacturer = STR_TO_DATE(@dateret, '%m/%d/%Y'),
        	baseline_date_first_marketed = STR_TO_DATE(@datefir, '%m/%d/%Y'),
        	baseline_date_ceased_marketing = STR_TO_DATE(@datecea, '%m/%d/%Y');
        """.format(file_path,"raw_device_"+update_type.lower())

        delete_sql = """delete t.* from {0} t""".format("raw_device_"+update_type.lower())

        cursor.execute(delete_sql)
        db.commit()


        cursor.execute(load_sql)
        db.commit()

    if file_name == "foitext":

        load_sql = """
        LOAD DATA LOCAL INFILE '{0}'
        INTO TABLE {1}
        FIELDS TERMINATED BY '|' IGNORE 1 LINES
        (mdr_report_key, mdr_narrative_key, narrative_type_code, patient_sequence_number, @datevar, narrative)
        SET date_report = STR_TO_DATE(@datevar, '%m/%d/%Y');
        """.format(file_path,"raw_narrative_"+update_type.lower())

        delete_sql = """delete t.* from {0} t""".format("raw_narrative_"+update_type.lower())

        cursor.execute(delete_sql)
        db.commit()


        cursor.execute(load_sql)
        db.commit()




if __name__ == '__main__':

    fda_data_processing()