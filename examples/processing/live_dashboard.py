import curses
import sys
import time

from rich.live import Live
from rich.table import Table


'''
Escape ANSI
'''
def print_dashboard(speed, passengers):
    sys.stdout.write("\033[H")  # vai in alto a sinistra
    print("=== BUS DASHBOARD ===")
    print(f"Velocità: {speed} km/h")
    print(f"Passeggeri: {passengers}")

def escape_ansi_method():
    print("\033[2J")  # pulisce lo schermo

    for i in range(50):
        print_dashboard(i, i*2)
        time.sleep(0.5)


'''
Curses
'''
def dashboard(stdscr):
    curses.curs_set(0)

    for i in range(100):
        stdscr.clear()
        stdscr.addstr(0, 0, "=== BUS DASHBOARD ===")
        stdscr.addstr(2, 0, f"Velocità: {i} km/h")
        stdscr.addstr(3, 0, f"Passeggeri: {i*2}")
        stdscr.refresh()
        time.sleep(0.5)

def curses_method():
    curses.wrapper(dashboard)


'''
Rich
'''
def generate_table(speed, passengers):
    table = Table(title="Bus Telemetry")
    table.add_column("Metric")
    table.add_column("Value")

    table.add_row("Velocità", f"{speed} km/h")
    table.add_row("Passeggeri", str(passengers))

    return table

def rich_method():
    with Live(generate_table(0, 0), refresh_per_second=2) as live:
        for i in range(100):
            live.update(generate_table(i, i*2))
            time.sleep(0.5)


def main():
    curses_method()

if __name__ == "__main__":
    main()
