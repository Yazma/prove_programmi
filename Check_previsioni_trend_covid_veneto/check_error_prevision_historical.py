import pandas as pd
import numpy as np
import datetime
import configparser
from send_mail import send_mail
import os

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

def calculate_difference(df1, df2):
    df_merged = df1.merge(df2,
                          how='inner',
                          left_on=['data'],
                          right_on=['data'])
    '''devo eliminare gli ultimi 2 giorni/righe perché i dati non sono considerati come consolidati'''
    # df_merged.drop(df_merged.tail(2).index, inplace=True)
    # df_merged.drop(df_merged.head(2).index, inplace=True) # nel caso volessi cancellare le prime 2 righe

    '''MAE: in questo case corrisponde alla differenza in modulo di previsione e storico perchè lavoro giorno per 
       giorno'''
    df_merged['MAE_T_C'] = abs(df_merged['T_C_p'] - df_merged['T_C_s'])
    df_merged['MAE_D'] = abs(df_merged['D_p'] - df_merged['D_s'])
    df_merged['MAE_T_P'] = abs(df_merged['T_P_p'] - df_merged['T_P_s'])
    df_merged['MAE_G'] = abs(df_merged['G_p'] - df_merged['G_s'])
    df_merged['MAE_T_I'] = abs(df_merged['T_I_p'] - df_merged['T_I_s'])

    '''NMAE'''
    df_merged['NMAE_T_C'] = (abs(df_merged['T_C_p'] - df_merged['T_C_s']) / df_merged['T_C_s']) * 100
    df_merged['NMAE_D'] = (abs(df_merged['D_p'] - df_merged['D_s']) / df_merged['D_s']) * 100
    df_merged['NMAE_T_P'] = (abs(df_merged['T_P_p'] - df_merged['T_P_s']) / df_merged['T_P_s']) * 100
    df_merged['NMAE_G'] = (abs(df_merged['G_p'] - df_merged['G_s']) / df_merged['G_s']) * 100
    df_merged['NMAE_T_I'] = (abs(df_merged['T_I_p'] - df_merged['T_I_s']) / df_merged['T_I_s']) * 100

    '''Serve perchè il primo elemento dei trend non può essere calcolato'''
    add_at_beginning_of_trend_series = pd.Series([np.nan])

    '''Trend storico'''
    df_merged['trend_T_C_s'] = add_at_beginning_of_trend_series.append(
        df_merged['T_C_s'][1:].reset_index(drop=True) - df_merged['T_C_s'][:-1].reset_index(drop=True)
                                                         ).reset_index(drop=True)
    df_merged['trend_D_s'] = add_at_beginning_of_trend_series.append(
        df_merged['D_s'][1:].reset_index(drop=True) - df_merged['D_s'][:-1].reset_index(drop=True)
                                                       ).reset_index(drop=True)
    df_merged['trend_T_P_s'] = add_at_beginning_of_trend_series.append(
        df_merged['T_P_s'][1:].reset_index(drop=True) - df_merged['T_P_s'][:-1].reset_index(drop=True)
                                                         ).reset_index(drop=True)
    df_merged['trend_G_s'] = add_at_beginning_of_trend_series.append(
        df_merged['G_s'][1:].reset_index(drop=True) - df_merged['G_s'][:-1].reset_index(drop=True)
                                                       ).reset_index(drop=True)
    df_merged['trend_T_I_s'] = add_at_beginning_of_trend_series.append(
        df_merged['T_I_s'][1:].reset_index(drop=True) - df_merged['T_I_s'][:-1].reset_index(drop=True)
                                                         ).reset_index(drop=True)

    '''Trend previsioni'''
    df_merged['trend_T_C_p'] = add_at_beginning_of_trend_series.append(
        df_merged['T_C_p'][1:].reset_index(drop=True) - df_merged['T_C_p'][:-1].reset_index(drop=True)
                                                         ).reset_index(drop=True)
    df_merged['trend_D_p'] = add_at_beginning_of_trend_series.append(
        df_merged['D_p'][1:].reset_index(drop=True) - df_merged['D_p'][:-1].reset_index(drop=True)
                                                       ).reset_index(drop=True)
    df_merged['trend_T_P_p'] = add_at_beginning_of_trend_series.append(
        df_merged['T_P_p'][1:].reset_index(drop=True) - df_merged['T_P_p'][:-1].reset_index(drop=True)
                                                         ).reset_index(drop=True)
    df_merged['trend_G_p'] = add_at_beginning_of_trend_series.append(
        df_merged['G_p'][1:].reset_index(drop=True) - df_merged['G_p'][:-1].reset_index(drop=True)
                                                       ).reset_index(drop=True)
    df_merged['trend_T_I_p'] = add_at_beginning_of_trend_series.append(
        df_merged['T_I_p'][1:].reset_index(drop=True) - df_merged['T_I_p'][:-1].reset_index(drop=True)
                                                         ).reset_index(drop=True)

    #.apply(lambda row: mean_absolute_error(row['T_C_p'], row['T_C_s']), axis=1)
    #.apply(lambda row: row['T_C_p'] + row['T_C_s'], axis=1)

    return df_merged


def check_err_storico_previsioni():
    path_file_cfg = os.path.abspath(os.path.join(__file__, "..", "..", "config", "config_WB.ini"))
    cfg = configparser.RawConfigParser()
    cfg.read(path_file_cfg)
    days = int(cfg['CHECK_PREV']['days'])

    suffix = '_previsto.csv'
    prefix_path_previsioni = '/home/osboxes/Scrivania/covid_veneto_data/'
    path_storico = '/home/osboxes/Scrivania/covid_veneto_data/input_covid.csv'

    name_dict_s = {'terapia_intensiva': 'T_I_s', 'totale_positivi': 'T_P_s', 'dimessi_guariti': 'G_s',
                   'deceduti': 'D_s', 'totale_casi': 'T_C_s'}
    name_dict_p = {'totale_casi': 'T_C_p', 'deceduti': 'D_p', 'totale_positivi_individuati': 'T_P_p',
                   'guariti_individuati': 'G_p', 'terapia_intensiva': 'T_I_p'}
    prev_col_to_drop = ['totale_casi_sigma', 'regione', 'deceduti_sigma', 'totale_casi_seir', 'deceduti_seir',
                        'totale_positivi_non_individuati', 'guariti_non_individuati', 'totale_casi_non_individuati',
                        'beta', 'gamma', 'R_0', 'beta_individuati', 'gamma_individuati', 'R_0_individuati',
                        'totale_casi_std', 'totale_positivi_individuati_std', 'guariti_individuati_std', 'deceduti_std',
                        'terapia_intensiva_std', 'totale_positivi_non_individuati_std', 'guariti_non_individuati_std',
                        'totale_casi_non_individuati_std', 'beta_std', 'gamma_std', 'R_0_std', 'beta_individuati_std',
                        'gamma_individuati_std', 'R_0_individuati_std', 'totale_casi_offset', 'deceduti_offset',
                        'guariti_offset', 'totale_positivi_offset', 'terapia_intensiva_offset', 'R_0_offset']
    dict_DF_err = {}

    path = os.path.abspath(advConf.INPUT_PATH)

    '''path locale e path di macchina'''
    df_storico = pd.read_csv(path_storico, sep=';')

    df_storico = df_storico.drop(['tamponi', 'popolazione', 'regione'], axis=1)
    df_storico = df_storico.rename(columns=name_dict_s, inplace=False)

    '''Formatto la data correttamente da 07/03/2021 a 2021-03-07 mantenendo la colonna di tipo stringa'''
    df_storico['data'] = pd.to_datetime(df_storico['data'], format='%d/%m/%Y')
    df_storico['data'] = df_storico['data'].dt.strftime('%Y-%m-%d')

    '''leggo l'ultima data presente nel file e la trasformo da string a datetime per poter sottrarre il numero di giorni
       indicati dal parametro'''
    last_data = df_storico.iloc[-1, :]['data']
    last_data = pd.to_datetime(last_data)

    for i in range(days, days+1):
        datetime.timedelta(i)
        file_date = last_data - datetime.timedelta(i)
        file_date = pd.to_datetime(file_date).strftime('%Y/%m/%d').replace('/', '_')
        file_name = file_date + suffix
        df_prev = pd.read_csv(os.path.join(prefix_path_previsioni, file_name), sep=',')
        df_prev = df_prev.drop(prev_col_to_drop, axis=1)
        df_prev = df_prev.rename(columns=name_dict_p, inplace=False)
        df_merged = calculate_difference(df_prev, df_storico)
        dict_DF_err['{}'.format(str(i))] = df_merged

    '''soglie per controllare le previsioni'''
    soglia_mae_T_C = 3000
    soglia_mae_D = 100
    soglia_mae_T_P = 2500
    soglia_mae_G = 3000
    soglia_mae_T_I = 15

    '''soglie percentuali'''
    soglia_nmae_T_C = 10  # significa il 10%
    soglia_nmae_D = 10
    soglia_nmae_T_P = 10
    soglia_nmae_G = 10
    soglia_nmae_T_I = 10

    text_for_mail_previsioni = 'Controllare le previsioni per uno o più campi tra: '
    text_for_mail_trend = 'Controllare il trend per uno o più campi tra: '
    yes_send_mail = ''
    yes_trend_send_mail = ''

    for i in range(days, days+1):
        df = dict_DF_err['{}'.format(i)]
        if df['MAE_T_C'][:-3].mean() > soglia_mae_T_C or df['NMAE_T_C'][:-3].mean() > soglia_nmae_T_C:
            yes_send_mail = True
            text_for_mail_previsioni = text_for_mail_previsioni + 'totale casi, '

        if df['MAE_D'][:-3].mean() > soglia_mae_D or df['NMAE_D'][:-3].mean() > soglia_nmae_D:
            yes_send_mail = True
            text_for_mail_previsioni = text_for_mail_previsioni + 'deceduti, '

        if df['MAE_T_P'][:-3].mean() > soglia_mae_T_P or df['NMAE_T_P'][:-3].mean() > soglia_nmae_T_P:
            yes_send_mail = True
            text_for_mail_previsioni = text_for_mail_previsioni + 'totale positivi, '

        if df['MAE_G'][:-3].mean() > soglia_mae_G or df['NMAE_G'][:-3].mean() > soglia_nmae_G:
            yes_send_mail = True
            text_for_mail_previsioni = text_for_mail_previsioni + 'guariti, '

        if df['MAE_T_I'][2:-1].mean() > soglia_mae_T_I or df['NMAE_T_I'][2:-1].mean() > soglia_nmae_T_I:
            yes_send_mail = True
            text_for_mail_previsioni = text_for_mail_previsioni + 'terpie intensive, '

        if (df['trend_T_C_s'][1:-3].mean() > 0 and df['trend_T_C_p'][1:-3].mean() < 0) or \
                (df['trend_T_C_s'][1:-3].mean() < 0 and df['trend_T_C_p'][1:-3].mean() > 0):
            yes_trend_send_mail = True
            text_for_mail_trend = text_for_mail_trend + 'totale casi, '

        if (df['trend_D_s'][1:-3].mean() > 0 and df['trend_D_p'][1:-3].mean() < 0) or \
                (df['trend_D_s'][1:-3].mean() < 0 and df['trend_D_p'][1:-3].mean() > 0):
            yes_trend_send_mail = True
            text_for_mail_trend = text_for_mail_trend + 'deceduti, '

        if (df['trend_T_P_s'][1:-3].mean() > 0 and df['trend_T_P_p'][1:-3].mean() < 0) or \
                (df['trend_T_P_s'][1:-3].mean() < 0 and df['trend_T_P_p'][1:-3].mean() > 0):
            yes_trend_send_mail = True
            text_for_mail_trend = text_for_mail_trend + 'totale positivi, '

        if (df['trend_G_s'][1:-3].mean() > 0 and df['trend_G_s'][1:-3].mean() < 0) or \
                (df['trend_G_s'][1:-3].mean() < 0 and df['trend_G_s'][1:-3].mean() > 0):
            yes_trend_send_mail = True
            text_for_mail_trend = text_for_mail_trend + 'guariti, '

        if (df['trend_T_I_s'][1:-3].mean() > 0 and df['trend_T_I_p'][1:-3].mean() < 0) or \
                (df['trend_T_I_s'][1:-3].mean() < 0 and df['trend_T_I_p'][1:-3].mean() > 0):
            yes_trend_send_mail = True
            text_for_mail_trend = text_for_mail_trend + 'terapie intensive, '

    text_for_mail = ''
    if yes_send_mail is True or yes_trend_send_mail is True:
        if yes_send_mail is True:
            text_for_mail += text_for_mail_previsioni + '\n'
        if yes_trend_send_mail is True:
            text_for_mail += text_for_mail_trend

        send_mail(text_for_mail)


if __name__ == '__main__':
    check_err_storico_previsioni()