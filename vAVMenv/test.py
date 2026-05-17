from autobus import Autobus
from autobus_elettrico import AutobusElettrico
from autobus_ibrido import AutobusIbrido
from autobus_termico import AutobusTermico
from autobus_elettrico import AutobusElettrico


# Test costruttore
def test_costruttore():
    flag = False

    try:
        Autobus(2, 3, 4, 5.0)
    except TypeError:
        flag = True
    
    return flag

# Test set_LP()
def test_set_LP(autobus: Autobus):
    flag = False

    try:
        autobus.set_LP(2.2)
    except TypeError:
        flag = True
    
    return flag

# Test set_timestamp()
def test_set_timestamp(autobus: Autobus):
    flag = False

    try:
        autobus.set_timestamp(2)
    except TypeError:
        flag = True
    
    return flag

# Test set_gps()
def test_set_gps(autobus: Autobus):
    flag = False

    try:
        autobus.set_gps(2)
    except TypeError:
        flag = True
    
    return flag

# Test set_longitude()
def test_set_longitude(autobus: Autobus):
    flag = False

    try:
        autobus.set_longitude(2)
    except TypeError:
        flag = True
    
    return flag

# Test set_latitude()
def test_set_latitude(autobus: Autobus):
    flag = False

    try:
        autobus.set_latitude(2)
    except TypeError:
        flag = True
    
    return flag

# Test set_speed()
def test_set_speed(autobus: Autobus):
    flag = False

    try:
        autobus.set_speed(2)
    except TypeError:
        flag = True
    
    return flag

# Test set_tyre_pressure()
def test_set_tyre_pressure(autobus: Autobus):
    flag = False

    try:
        autobus.set_tyre_pressure(2)
    except TypeError:
        flag = True
    
    return flag

# Test set_brake_status()
def test_set_brake_status(autobus: Autobus):
    flag = False

    try:
        autobus.set_brake_status(2)
    except TypeError:
        flag = True
    
    return flag

# Test set_engine_status()
def test_set_engine_status(autobus: Autobus):
    flag = False

    try:
        autobus.set_engine_status(2)
    except TypeError:
        flag = True
    
    return flag

# Test set_num_psg()
def test_set_num_psg(autobus: Autobus):
    flag = False

    try:
        autobus.set_num_psg(2.2)
    except TypeError:
        flag = True
    
    return flag

# Test set_environmental_data()
def test_set_environmental_data(autobus: Autobus):
    flag = False

    try:
        autobus.set_environmental_data(2)
    except TypeError:
        flag = True
    
    return flag

# Test set_temperature()
def test_set_temperature(autobus: Autobus):
    flag = False

    try:
        autobus.set_temperature(2)
    except TypeError:
        flag = True
    
    return flag

# Test set_humidity()
def test_set_humidity(autobus: Autobus):
    flag = False

    try:
        autobus.set_humidity(2)
    except TypeError:
        flag = True
    
    return flag

# Test set_lat_direction()
def test_lat_direction(autobus: Autobus):
    flag = False

    try:
        autobus.set_lat_direction(2)
    except TypeError:
        flag = True

    return flag

# Test set_long_direction()
def test_long_direction(autobus: Autobus):
    flag = False

    try:
        autobus.set_long_direction(2)
    except TypeError:
        flag = True

    return flag

# Test set_formatted_data_to_send()
def test_set_formatted_data_to_send(autobus: Autobus):
    flag = False

    try:
        autobus.set_formatted_data_to_send(2)
    except TypeError:
        flag = True
    
    return flag

# Test get_updated_data()
def test_get_updated_data(autobus: AutobusTermico | AutobusIbrido | AutobusElettrico):
    flag = False

    dictionary = {
        "fuel_level": 479.0,
        "fuel_consumption": 1.0
    }

    autobus.set_updated_data(dictionary)

    dictionary["fuel_level"] = 478.0
    dictionary["fuel_consumption"] = 2.0

    diz_autobus = autobus.get_updated_data()

    if diz_autobus["fuel_level"] == 479.0 and diz_autobus["fuel_consumption"] == 1.0:
        flag = True

    return flag

# Test set_updated_data()
def test_set_updated_data(autobus: AutobusTermico | AutobusIbrido | AutobusElettrico):
    flag = False

    try:
        autobus.set_updated_data(2)
    except TypeError:
        flag = True
    
    return flag

# Test get_threshold_list()
def test_get_threshold_list(autobus: AutobusTermico | AutobusIbrido | AutobusElettrico):
    flag = False

    lista = [0.0, 1.0, 2.0, 3.0, 4.0]

    autobus.set_threshold_list(lista)

    lista.append(5.0)

    lista_autobus = autobus.get_threshold_list()

    if len(lista_autobus) == 5:
        flag = True

    return flag

# Test set_threshold_list()
def test_set_threshold_list(autobus: AutobusTermico | AutobusIbrido | AutobusElettrico):
    flag = False

    try:
        autobus.set_threshold_list(2)
    except TypeError:
        flag = True
    
    return flag

# Test set_static_threshold()
def test_set_static_threshold(autobus: AutobusTermico | AutobusIbrido | AutobusElettrico, stat_thres: float):
    flag = False

    autobus.set_static_threshold(stat_thres)
    thres_list = autobus.get_threshold_list()

    if len(thres_list) == int(stat_thres)+1:
        flag = True
    
    return flag

# Test simulate()
def test_simulate(autobus: Autobus, long_low: float, long_up: float, lat_low: float, lat_up: float, speed_low: float, speed_up: float, tyre_low: float, tyre_up: float, psg_low: int, psg_up: int, temp_low: float, temp_up: float, hum_low: float, hum_up: float, brake_range: list, engine_range: list, flag_exec: bool, cont_fermate: int):
    flag = True

    autobus.simulate(flag_exec, cont_fermate)

    long = autobus.get_longitude()
    lat = autobus.get_latitude()
    speed = autobus.get_speed()
    tyre = autobus.get_tyre_pressure()
    psg = autobus.get_num_psg()
    temp = autobus.get_temperature()
    hum = autobus.get_humidity()
    brake = autobus.get_brake_status()
    engine = autobus.get_engine_status()

    if flag and ( long > long_up or long < long_low ):
        flag = False
    
    if flag and ( lat > lat_up or lat < lat_low ):
        flag = False

    if flag and ( speed > speed_up or speed < speed_low ):
        flag = False

    if flag and ( tyre > tyre_up or tyre < tyre_low ):
        flag = False

    if flag and ( psg > psg_up or psg < psg_low ):
        flag = False

    if flag and ( temp > temp_up or temp < temp_low ):
        flag = False

    if flag and ( hum > hum_up or hum < hum_low ):
        flag = False

    if flag and brake not in brake_range:
        flag = False

    if flag and engine not in engine_range:
        flag = False

    return flag

# Test Costruttore AutobusIbrido
def test_costruttore_hybrid(num_autobus: int, ranges: dict, timeout: float, host: str, port: int):
    license_p_list = []

    for _ in range(0, num_autobus):
        a = AutobusIbrido(ranges, timeout, host, port)
        license_p_list.append(a.get_LP())
    
    flag = True
    for i in range(0, num_autobus):
        for j in range(i+1, num_autobus):
            if license_p_list[i] == license_p_list[j]:
                flag = False

    return flag

# Metodo dedicato all'esecuzione dei test UD, TL e ST in modo indipendente dal tipo di motorizzazioe dell'autobus passato
#   UD --> Updated Data
#   TL --> Threshold List
#   ST --> Static Threshold
def call_UDTLST_tests(autobus: AutobusTermico | AutobusIbrido | AutobusElettrico):
    stat_thres_greater = 80.0
    stat_thres_lower = 50.0
    stat_thres_equal = 75.0

    flag_gud = test_get_updated_data(autobus)
    flag_sud = test_set_updated_data(autobus)
    flag_gtl = test_get_threshold_list(autobus)
    flag_stl = test_set_threshold_list(autobus)
    flag_sstg = test_set_static_threshold(autobus, stat_thres_greater)
    flag_sstl = test_set_static_threshold(autobus, stat_thres_lower)
    flag_sste = test_set_static_threshold(autobus, stat_thres_equal)

    return flag_gud, flag_sud, flag_gtl, flag_stl, flag_sstg, flag_sstl, flag_sste

# Metodo dedicato alla stampa di report dei test UD, TL e ST in modo indipendente dal tipo di motorizzazioe
# dell'autobus passato
#   UD --> Updated Data
#   TL --> Threshold List
#   ST --> Static Threshold
def report_UDTLST_tests(flag_gud, flag_sud, flag_gtl, flag_stl, flag_sstg, flag_sstl, flag_sste):
    print("\tTest get_updated_data(): " + "SUPERATO" if flag_gud else "NON SUPERATO")
    print("\tTest set_updated_data(): " + "SUPERATO" if flag_sud else "NON SUPERATO")
    print("\tTest get_threshold_list(): " + "SUPERATO" if flag_gtl else "NON SUPERATO")
    print("\tTest set_threshold_list(): " + "SUPERATO" if flag_stl else "NON SUPERATO")
    print("\tTest set_static_threshold() greater than constructor: " + "SUPERATO" if flag_sstg else "NON SUPERATO")
    print("\tTest set_static_threshold() lower than constructor: " + "SUPERATO" if flag_sstl else "NON SUPERATO")
    print("\tTest set_static_threshold() equal to constructor: " + "SUPERATO" if flag_sste else "NON SUPERATO")

# Test robustezza metodo simulate() per evitare problemi nell'evenutalità di errore nella chiamata o di errato utilizzo
# semantico, ossia che l'utilizzo non avvenga all'interno di un ciclo
def test_robustness_simulate(ranges: dict, timeout: float, host: str, port: int, long_low: float, long_up: float, lat_low: float, lat_up: float, tyre_low: float, tyre_up: float, psg_low: int, psg_up: int, brake_range: list, engine_range: list, bt_temp_low: float, bt_temp_up: float, cont_fermate: int):
    flag = True

    termic_autobus = AutobusTermico(ranges=ranges, timeout=timeout, host=host, port=port)
    hybrid_autobus = AutobusIbrido(ranges=ranges, timeout=timeout, host=host, port=port)
    electric_autobus = AutobusElettrico(ranges=ranges, timeout=timeout, host=host, port=port)

    hybrid_autobus.simulate(False, cont_fermate)
    if flag and ( (hybrid_autobus.get_latitude() < lat_low or hybrid_autobus.get_latitude() > lat_up) or (hybrid_autobus.get_longitude() < long_low or hybrid_autobus.get_longitude() > long_up) or (hybrid_autobus.get_tyre_pressure() < tyre_low or hybrid_autobus.get_tyre_pressure() > tyre_up) or (hybrid_autobus.get_num_psg() < psg_low or hybrid_autobus.get_num_psg() > psg_up) or (hybrid_autobus.get_brake_status() not in [brake_range[-3], brake_range[-2], brake_range[-1]]) or (hybrid_autobus.get_engine_status() not in [engine_range[-3], engine_range[-2], engine_range[-1]]) or (hybrid_autobus.get_battery_temp() < bt_temp_low or hybrid_autobus.get_battery_temp() > bt_temp_up) ):
        flag = False
    
    electric_autobus.simulate(False, cont_fermate)
    if flag and ( (electric_autobus.get_latitude() < lat_low or electric_autobus.get_latitude() > lat_up) or (electric_autobus.get_longitude() < long_low or electric_autobus.get_longitude() > long_up) or (electric_autobus.get_tyre_pressure() < tyre_low or electric_autobus.get_tyre_pressure() > tyre_up) or (electric_autobus.get_num_psg() < psg_low or electric_autobus.get_num_psg() > psg_up) or  (electric_autobus.get_brake_status() not in [brake_range[-3], brake_range[-2], brake_range[-1]]) or (electric_autobus.get_engine_status() not in [engine_range[-3], engine_range[-2], engine_range[-1]]) or (electric_autobus.get_battery_temp() < bt_temp_low or electric_autobus.get_battery_temp() > bt_temp_up) ):
        flag = False

    termic_autobus.simulate(False, cont_fermate)
    if flag and ( (termic_autobus.get_latitude() < lat_low or termic_autobus.get_latitude() > lat_up) or (termic_autobus.get_longitude() < long_low or termic_autobus.get_longitude() > long_up) or (termic_autobus.get_tyre_pressure() < tyre_low or termic_autobus.get_tyre_pressure() > tyre_up) or (termic_autobus.get_num_psg() < psg_low or termic_autobus.get_num_psg() > psg_up) or (termic_autobus.get_brake_status() not in [brake_range[-3], brake_range[-2], brake_range[-1]]) or (termic_autobus.get_engine_status() not in [engine_range[-3], engine_range[-2], engine_range[-1]]) ):
        flag = False

    return flag


# Method main() - metodo che consente di eseguire i test progettati
def main():
    # Ranges intervallo misure
    ranges = {
        #       GPS
        "gps": {
            #   [°N]
            "latitude_low": 44.49321,
            "latitude_up": 44.83591,
            #   [°E]
            "longitude_low": 11.27662,
            "longitude_up": 11.61932
        },
        #       [km/h]
        "speed_low": 0.0,
        "speed_up": 100.0,
        #       [bar]
        "tyre_pressure_low": 1.0,
        "tyre_pressure_up": 4.5,
        #       Brake Status
        "brake_status": ["pessimo", "mediocre", "cattivo", "accettabile", "buono", "ottimo", "eccellente"],
        #       Engine Status
        "engine_status": ["pessimo", "mediocre", "cattivo", "accettabile", "buono", "ottimo", "eccellente"],
        #       [persone]
        "num_psg_low": 0,
        "num_psg_up": 75,
        "environmental": {
            #   [°C]
            "temp_low": -5.0,
            "temp_up": 30.0,
            #   [%]
            "hum_low": 0.0,
            "hum_up": 100.0
        },
        #       [%]
        "battery_lvl_low": 0.0,
        "battery_lvl_up": 100.0,
        #       [°C]
        "battery_temp_low": 5.0,
        "battery_temp_up": 55.0,
        #       [l]
        "termic_fuel_lvl_low": 0.0,
        "termic_fuel_lvl_up": 480.0,
        #       [l]
        "termic_fuel_cons_low": 0.0,
        "termic_fuel_cons_up": 480.0,
        #       [l]
        "hybrid_fuel_lvl_low": 0.0,
        "hybrid_fuel_lvl_up": 400.0,
        #       [l]
        "hybrid_fuel_cons_low": 0.0,
        "hybrid_fuel_cons_up": 400.0
    }
    # Setup timeout attesa pubblicazione messaggio broker MQTT
    delay_mqtt = 4.90
    # Host MQTT broker
    host = "localhost"
    # Porta MQTT broker
    port = 1883 
    # Istanze di autobus necessaria per i test da condurre
    autobus = Autobus(ranges=ranges, timeout=delay_mqtt, host=host, port=port)
    autobus_termico = AutobusTermico(ranges=ranges, timeout=delay_mqtt, host=host, port=port)
    autobus_ibrido = AutobusIbrido(ranges=ranges, timeout=delay_mqtt, host=host, port=port)
    autobus_elettrico = AutobusElettrico(ranges=ranges, timeout=delay_mqtt, host=host, port=port)

    # Conduzione test
    flag_costruttore = test_costruttore()
    flag_ID = test_set_LP(autobus)
    flag_time = test_set_timestamp(autobus)
    flag_gps = test_set_gps(autobus)
    flag_longitude = test_set_longitude(autobus)
    flag_latitude = test_set_latitude(autobus)
    flag_speed = test_set_speed(autobus)
    flag_tyre_pressure = test_set_tyre_pressure(autobus)
    flag_brake_status = test_set_brake_status(autobus)
    flag_engine_status = test_set_engine_status(autobus)
    flag_num_psg = test_set_num_psg(autobus)
    flag_env_data = test_set_environmental_data(autobus)
    flag_temperature = test_set_temperature(autobus)
    flag_humidity = test_set_humidity(autobus)
    flag_lat_direction = test_lat_direction(autobus)
    flag_long_direction = test_long_direction(autobus)
    flag_f_data = test_set_formatted_data_to_send(autobus)
    flag_sim = test_simulate(autobus, ranges["gps"]["longitude_low"], ranges["gps"]["longitude_up"], ranges["gps"]["latitude_low"], ranges["gps"]["latitude_up"], ranges["speed_low"], ranges["speed_up"], ranges["tyre_pressure_low"], ranges["tyre_pressure_up"], ranges["num_psg_low"], ranges["num_psg_up"], ranges["environmental"]["temp_low"], ranges["environmental"]["temp_up"], ranges["environmental"]["hum_low"], ranges["environmental"]["hum_up"], ranges["brake_status"], ranges["engine_status"], True, 1)
    # Attenzione!!! Numero di targhe passato non massimo perché altrimenti non si riesce a condurre altri test dato il numero
    # limitato di targhe, in ogni caso il test condotto qui è quello di verificare se tutte le targhe assegnate agli autobus
    # sono differenti l'un l'altra, quindi anche se non si raggiunge saturazione va bene lo stesso anche condotto su un numero
    # minore di autobus
    flag_costruttore_hybrid = test_costruttore_hybrid(8, ranges, delay_mqtt, host, port)
    flag_gud_t, flag_sud_t, flag_gtl_t, flag_stl_t, flag_sstg_t, flag_sstl_t, flag_sste_t = call_UDTLST_tests(autobus_termico)
    flag_gud_i, flag_sud_i, flag_gtl_i, flag_sthl_i, flag_sstg_i, flag_sstl_i, flag_sste_i = call_UDTLST_tests(autobus_ibrido)
    flag_gud_e, flag_sud_e, flag_gtl_e, flag_sthl_e, flag_sstg_e, flag_sstl_e, flag_sste_e = call_UDTLST_tests(autobus_elettrico)
    flag_robustness = test_robustness_simulate(ranges, delay_mqtt, host, port, ranges["gps"]["longitude_low"], ranges["gps"]["longitude_up"], ranges["gps"]["latitude_low"], ranges["gps"]["latitude_up"], ranges["tyre_pressure_low"], ranges["tyre_pressure_up"], ranges["num_psg_low"], ranges["num_psg_up"], ranges["brake_status"], ranges["engine_status"], ranges["battery_temp_low"], ranges["battery_temp_up"], 1)

    # Reporting test
    print("Reporting test condotti:")
    print("\tTest costruttore: " + "SUPERATO" if flag_costruttore else "NON SUPERATO")
    print("\tTest set_LP(): " + "SUPERATO" if flag_ID else "NON SUPERATO")
    print("\tTest set_timestamp(): " + "SUPERATO" if flag_time else "NON SUPERATO")
    print("\tTest set_gps(): " + "SUPERATO" if flag_gps else "NON SUPERATO")
    print("\tTest set_longitude(): " + "SUPERATO" if flag_longitude else "NON SUPERATO")
    print("\tTest set_latitude(): " + "SUPERATO" if flag_latitude else "NON SUPERATO")
    print("\tTest set_speed(): " + "SUPERATO" if flag_speed else "NON SUPERATO")
    print("\tTest set_tyre_pressure(): " + "SUPERATO" if flag_tyre_pressure else "NON SUPERATO")
    print("\tTest set_brake_status(): " + "SUPERATO" if flag_brake_status else "NON SUPERATO")
    print("\tTest set_engine_status(): " + "SUPERATO" if flag_engine_status else "NON SUPERATO")
    print("\tTest set_num_psg(): " + "SUPERATO" if flag_num_psg else "NON SUPERATO")
    print("\tTest set_environmental_data(): " + "SUPERATO" if flag_env_data else "NON SUPERATO")
    print("\tTest set_temperature(): " + "SUPERATO" if flag_temperature else "NON SUPERATO")
    print("\tTest set_humidity(): " + "SUPERATO" if flag_humidity else "NON SUPERATO")
    print("\tTest set_lat_direction(): " + "SUPERATO" if flag_lat_direction else "NON SUPERATO")
    print("\tTest set_long_direction(): " + "SUPERATO" if flag_long_direction else "NON SUPERATO")
    print("\tTest set_formatted_data_to_send(): " + "SUPERATO" if flag_f_data else "NON SUPERATO")
    print("\tTest simulate(): " + "SUPERATO" if flag_sim else "NON SUPERATO")
    print("\tTest costruttore hybrid: " + "SUPERATO" if flag_costruttore_hybrid else "NON SUPERATO")
    print("\tTest robustezza simulate(): " + "SUPERATO" if flag_robustness else "NON SUPERATO")
    print("\tTermico")
    report_UDTLST_tests(flag_gud_t, flag_sud_t, flag_gtl_t, flag_stl_t, flag_sstg_t, flag_sstl_t, flag_sste_t)
    print("\tIbrido")
    report_UDTLST_tests(flag_gud_i, flag_sud_i, flag_gtl_i, flag_sthl_i, flag_sstg_i, flag_sstl_i, flag_sste_i)
    print("\tElettrico")
    report_UDTLST_tests(flag_gud_e, flag_sud_e, flag_gtl_e, flag_sthl_e, flag_sstg_e, flag_sstl_e, flag_sste_e)

    # Reporting test semantica set_lat_direction() e set_long_direction()
    autobus.set_lat_direction("nord")
    print("\tTest nord: " + autobus.get_lat_direction())
    autobus.set_lat_direction("sud")
    print("\tTest sud: " + autobus.get_lat_direction())
    autobus.set_lat_direction("NORD")
    print("\tTest NORD: " + autobus.get_lat_direction())
    autobus.set_lat_direction("SUD")
    print("\tTest SUD: " + autobus.get_lat_direction())

    autobus.set_long_direction("est")
    print("\tTest est: " + autobus.get_long_direction())
    autobus.set_long_direction("ovest")
    print("\tTest ovest: " + autobus.get_long_direction())
    autobus.set_long_direction("EST")
    print("\tTest EST: " + autobus.get_long_direction())
    autobus.set_long_direction("OVEST")
    print("\tTest OVEST: " + autobus.get_long_direction())

    autobus.set_lat_direction("abc")
    print("\tTest abc: " + autobus.get_lat_direction())
    autobus.set_long_direction("abc")
    print("\tTest abc: " + autobus.get_long_direction())


if __name__ == "__main__":
    main()
