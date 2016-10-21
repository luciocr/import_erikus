# -------------------------------------------------------------------------------
# Name:        modulo2
# Purpose:
#
# Author:      lucicrua
#
# Created:     07/10/2016
# Copyright:   (c) lucicrua 2016
# Licence:     <your licence>
# -------------------------------------------------------------------------------


def main():
    # sistema = platform.system() #dice quale S.O. e' in esecuzione cosi' si puo' cambiare l'acapo
    sistema = platform.system()
    a_capo = '\n'
    if sistema == 'Windows':
        a_capo = '\r\n'

    comuni_diz = {}

    # dbPG = psycopg2.connect("dbname='dpcn' user='c_dati' host='192.168.1.111' password='c_dati12'")
    dbPG = psycopg2.connect("dbname='dpcn' user='c_dati' host='virtcsi-sc22.arpa.piemonte.it' password='c_dati12'")
    datiPG = dbPG.cursor()

    # creo un dizionario tramite la tabella "import_comuni" per avere il criterio di ricerca, il corrispondente nome della cartella, il codice ISTAT e il nome esteso
    q_comuni = 'SELECT impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome FROM import.import_comuni;'
    datiPG.execute(q_comuni)
    for pp in datiPG:
        comuni_diz[pp[1]] = (pp[0], pp[2], pp[3])

    ##    with open(r'D:\temp\comuni.txt') as f:
    ##        comuni_diz=dict([line.rstrip().split(',') for line in f])

    nome_file = os.path.join('/tmp', 'elenco_file_arrivati.csv')  # in questo file memorizzo le mail arrivate
    csvfile = open(nome_file, 'wb')

    mailfile = open('/tmp/mail_notfind.txt', 'wb')
    mail_notfound = open('/tmp/mail_notfind.csv', 'wb')
    #    fieldnames = ['mail', 'allegato','file_zippati']
    csvwrtr = csv.writer(csvfile, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    mailcsv = csv.writer(mail_notfound, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    # csvwrtr.writeheader()

    os.chdir('/tmp')
    for elem in os.listdir(os.curdir):
        if elem.find('.') > 0:
            if elem.rsplit('.', 1)[1] == 'tgz':
                zimbra_file = os.path.join(os.curdir, elem)
                break
            #    zimbra_file=r"D:\Downloads\0_sigeo-2016-10-13-121752.tgz"
            #    tar = tarfile.open(r"D:\Downloads\0_sigeo-2016-10-13-152943.tgz", "r:gz")
    tar = tarfile.open(zimbra_file, "r:gz")  # apro il file salvaTo da zimbra
    #    tar = tarfile.open(r"D:\Downloads\mail_zip.tgz", "r:gz")
    comuni_trovati = []
    for mail in tar:
        comune = []
        if mail.size > 5000:
            tar.extract(mail, '/tmp')
            msg = email.message_from_file(
                open(os.path.join('/tmp', os.path.dirname(mail.name), os.path.basename(mail.name))))
            nome_mail = (os.path.basename(mail.name))
            mail_ogg = msg['Subject']
            mail_da = msg['From']
            mail_data = msg['Date'][5:25]
            for comune_el in comuni_diz.keys():  # con questo ciclo cerco il nome del comune in tutte le parti della mail compresi i nomi degli allegati, ma non nel testo della mail
                comune_ky = [s for s in msg.items.im_self.values() if comune_el in s.lower()]
                ## if (comune_el in s for s in msg.items.im_self.values()): #comune=[s for s in msg.items.im_self.values() if comune_el in s]
                if comune_ky <> []:
                    #                    comuni_trovati.append(comuni_diz[comune_el])
                    istat = comuni_diz[comune_el][0]
                    cart_com = comuni_diz[comune_el][1]
                    comune = comuni_diz[comune_el][2]
                    break
                else:
                    for alleg in msg.walk():
                        if alleg.get_filename() is not None:
                            if comune_el in alleg.get_filename().lower():
                                istat = comuni_diz[comune_el][0]
                                cart_com = comuni_diz[comune_el][1]
                                comune = comuni_diz[comune_el][2]
                                break
                                # ][s for s in alleg.get_filename() #
            if comune <> []:

                # ??PER ESSERE SICURI DI COPIARE SEMPRE L'ULTIMO FILE FORSE SAREBBE MEGLIO COPIARE/SCOMPATTARE I FILE IN DIR TEMPORANEA E POI SE IL DBF E' PIU' RECENTE COPIARE I FILE NELLA GIUSTA DIRECTORY??

                csvwrtr.writerow([nome_mail, mail_ogg, mail_data, mail_da])
                for part in msg.walk():  # navigo le varie parti della mail per trovare gli allegati e salvarli
                    if comune == [] and part.get_content_type() == 'text/plain':  # se non ho ancora trovato il nome del comune lo cerco nel testo della mail
                        #                        mailfile.write(part.get_payload())
                        if comune_el in part.get_payload():  # prints the raw text
                            istat = comuni_diz[comune_el][0]
                            cart_com = comuni_diz[comune_el][1]
                            comune = comuni_diz[comune_el][2]
                    nome_file = part.get_filename()  # se la parte e' un allegato acquisisco il nome
                    if decode_header(nome_file)[0][1] is not None:
                        nome_file = str(decode_header(nome_file)[0][0]).decode(decode_header(nome_file)[0][
                                                                                   1])  # decodifico il file in modo che non abbia caratteri incompatibili con un nome file...
                    if nome_file is not None:  # salvo ed estraggo (se e' un file compresso) gli allegati
                        estens = nome_file.rsplit('.', 1)[
                            1]  # restituisce l'estensione del file partendo da destra taglia i primi caratteri fino al punto
                        nome_file = nome_file.expandtabs(1)  # elimino ulteriori caratteri strani dal nome del file
                        nome_file = nome_file.replace('\n', '')  # elimino possibili a capo dal nome del file
                        csvwrtr.writerow(['', nome_file])
                        extr_dir = os.path.join('/tmp', os.path.dirname(mail.name),
                                                cart_com)  # imposto la directory di estrazione o di copia dei file
                        if not os.path.exists(extr_dir):
                            os.mkdir(extr_dir)  # creo la directory di estrazione o di copia dei file
                        extr_file = (os.path.join(extr_dir, nome_file))
                        fp = open(extr_file, 'wb')
                        fp.write(part.get_payload(decode=True))  # scrivo l'allegato nella dir
                        fp.close()
                        # se l'allegato e' un file compresso ora ci sono una serie di metodi di estrazione per ogni tipo di file
                        if estens == 'zip':
                            zip = zipfile.ZipFile(extr_file)  # ,'r')
                            for each_f in zip.namelist():
                                #                            print "Extracting " + each_f
                                # zip.extract(each_f,extr_dir)
                                csvwrtr.writerow(['', '', each_f])
                        elif estens == 'rar':
                            rar = rarfile.RarFile(extr_file)
                            for each_f in rar.infolist():
                                #                            print "Extracting " + each_f.filename
                                csvwrtr.writerow(['', '', '', '', '', each_f.filename])
                                #                            rar.extract(each_f,extr_dir)
                        elif estens == '7z':
                            print "7z Extracting " + Archive(extr_file).filename
                            csvwrtr.writerow(['', nome_file])
                            Archive(extr_file).extractall(extr_dir)
                        elif estens == 'tgz':
                            tar = tarfile.open(extr_file, "r:gz")
                            for file_z in tar:
                                tar.extract(file_z, extr_dir)
                        else:
                            if estens == 'dbf':
                                data_dbf = datetime.datetime.strftime(
                                    datetime.datetime.fromtimestamp(os.path.getmtime(extr_file)), '%Y-%m-%d %H:%M:%S')
                                #                        print "file non compresso " + nome_file
                            csvwrtr.writerow(['', nome_file])
                        #                csvwrtr.writerow([comune,nome_mail,mail_ogg,mail_data,mail_da]) #qui dovrebbe essere scritto il LOG nel DB
            else:
                mailcsv.writerow([nome_mail, mail_ogg, mail_data, mail_da])
                mailfile.writelines("%s\n" % list(i) for i in msg.items())
                mailfile.write('\n-+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+\n')
                #                mailfile.write(msg.as_string())
                mailfile.write('\n--------------\n\n')
            #                continue
            #        csvwrtr.writerow([nome_mail,mail_ogg,mail_data,mail_da,nome_file])
    mailfile.writelines("%s\n" % comuni_trovati)
    mailfile.write('\n-+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+\n')
    mailfile.write('\n-+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+*+\n')
    mailfile.writelines("%s\n" % dict(Counter(comuni_trovati)))
    csvfile.close()
    mailfile.close()
    #    print (comuni_trovati)
    print dict(Counter(comuni_trovati))


if __name__ == '__main__':
    import tarfile, zipfile, \
        rarfile  # rarfile per funzionare ha bisogno dell'eseguibile la linea di comando di unrar.exe. Deve essere in una directory nel path del pc, ad esempio C:\python
    import csv  # , string
    import psycopg2, datetime
    from collections import Counter
    from pyunpack import \
        Archive  # per funzionare ha bisogno dell'eseguibile la linea di comando di unrar.exe e 7zip.exe. Devono essere in una directory nel path del pc, ad esempio C:\python
    from email.header import decode_header
    import email, sys, os, platform

    main()


#
# CREATE TABLE import.import_comuni
# (
#  impcom_id serial NOT NULL,
#  impcom_istat character varying(8),
#  impcom_str_ricerca character varying(100),
#  impcom_cartella character varying(100),
#  impcom_nome character varying(150),
#  CONSTRAINT impcom_pk PRIMARY KEY (impcom_id)
# )
# WITH (
#  OIDS=FALSE
# );
# ALTER TABLE import.import_comuni
#  OWNER TO _dpcn;
#
#
# CREATE TABLE import.import_log
# (
#  implog_id serial NOT NULL,
#  implog_comune character varying(100),
#  implog_istat character varying(8),
#  implog_mail_ogg character varying(500),
#  implog_mail_from character varying(100),
#  implog_mail_data date,
#  implog_file_compr character varying(100),
#  implog_data_filecompr date,
#  implog_data_dbfshp date,
#  CONSTRAINT implog_pk PRIMARY KEY (implog_id)
# )
# WITH (
#  OIDS=FALSE
# );
# ALTER TABLE import.import_log
#  OWNER TO _dpcn;
#
#
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('12057001','accumoli','accumoli','Accumoli');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043001','acquacanina','acquacanina','Acquacanina ');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11044001','acquasanta','acquasantaterme','Acquasanta Terme');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11109002','amandola','amandola','Amandola');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('12057006','amatrice','amatrice','Amatrice');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11044005','appignano','appignanodeltronto','Appignano del Tronto');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11044006','arquata','arquatadeltronto','Arquata del Tronto');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043007','ascoli','ascolipiceno','Ascoli Piceno');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043004','belforte','belfortedelchienti','Belforte del Chienti');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11109003','belmonte','belmontepiceno','Belmonte Piceno');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043006','caldarola','caldarola','Caldarola');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('10054007','cascia','cascia','Cascia');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043010','castelsantangelo','castelsantangelo','Castelsantangelo sul Nera');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11044013','castorano','castorano','Castorano');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('12057017','cittareale','cittareale','Cittareale');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('13067018','colledara','colledara','Colledara');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043014','colmurano','colmurano','Colmurano');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11044015','comunanza','comunanza','Comunanza');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043020','folignano','folignano','Folignano');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043021',' force','force','Force');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043022','force ','force','Force');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11109008','grottazzolina','grottazzolina','Grottazzolina');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('12057033','leonessa','leonessa','Leonessa');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043022','loropiceno','loropiceno','Loro Piceno');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11109010','magliano','maglianodeltenna','Magliano del Tenna');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043024','matelica','matelica','Matelica');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11044032','montalto','montaltodellemarche','Montalto delle Marche');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11109015','montefortino','montefortino','Montefortino');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11109019','monteleone','monteleonedispoleto','Monteleone di Spoleto');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11044044','montemonaco','montemonaco','Montemonaco');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('13066056','montereale','montereale','Montereale');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11109024','monteurano','monteurano','Monte Urano');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('10054035','norcia','norcia','Norcia');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043037','bovigliana','pievebovigliana','Pievebovigliana');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043039','pioraco','pioraco','Pioraco');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('13066072','pizzoli','pizzoli','Pizzoli');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11109015','preci','preci','Preci');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043045','ripe','ripesanginesio','Ripe San Ginesio');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043043','tolentino','tolentino','Tolentino');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11043053','urbisaglia','urbisaglia','Urbisaglia');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('13067046','castellana','vallecastellana','Valle Castellana');
# INSERT INTO import.import_comuni(impcom_istat, impcom_str_ricerca, impcom_cartella, impcom_nome) VALUES ('11044073','venarotta','venarotta','Venarotta');
