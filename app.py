import tkinter as tk
from tkinter import filedialog, scrolledtext, messagebox, ttk
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchWindowException, WebDriverException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.microsoft import EdgeChromiumDriverManager
import urllib.parse
import random
import time
import threading
import os
import tempfile
import platform
import logging

# Default Timer Values
TIMER_MIN = 5
TIMER_MAX = 15

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("whatsapp_blaster.log"),
        logging.StreamHandler()
    ]
)

# Determine a persistent folder in the %temp% directory
def get_persistent_temp_path():
    temp_dir = tempfile.gettempdir()
    persistent_temp_path = os.path.join(temp_dir, "whatsapp_blaster_data")
    if not os.path.exists(persistent_temp_path):
        os.makedirs(persistent_temp_path)
    return persistent_temp_path

# User data path
user_data_path = get_persistent_temp_path()

# Global stop event
stop_event = threading.Event()

class WhatsAppBlaster:
    def __init__(self):
        self.driver = None

    def setup_browser(self, headless=False):
        options = Options()
        options.add_argument(f"--user-data-dir={user_data_path}")
        options.add_argument("--remote-debugging-port=9222")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-background-networking")
        options.add_argument("--disable-default-apps")
        options.add_argument("--disable-popup-blocking")
        options.add_argument("--no-first-run")
        options.add_argument("--no-service-autorun")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")

        if headless:
            options.add_argument("--headless")

        browser_path, browser_type = self.locate_browser()
        if not browser_path:
            raise RuntimeError("No supported browser (Chrome, Brave, Edge) found on the system.")
        options.binary_location = browser_path

        if browser_type == "chrome":
            service = Service(ChromeDriverManager().install())
        elif browser_type == "edge":
            service = Service(EdgeChromiumDriverManager().install())
        else:
            service = Service(ChromeDriverManager().install())

        self.driver = webdriver.Chrome(service=service, options=options)
        return self.driver

    def locate_browser(self):
        system = platform.system()
        if system == "Windows":
            brave_paths = [
                os.path.expandvars(r"%ProgramFiles%\BraveSoftware\Brave-Browser\Application\brave.exe"),
                os.path.expandvars(r"%ProgramFiles(x86)%\BraveSoftware\Brave-Browser\Application\brave.exe"),
                os.path.expandvars(r"%LocalAppData%\BraveSoftware\Brave-Browser\Application\brave.exe"),
            ]
            chrome_paths = [
                os.path.expandvars(r"%ProgramFiles%\Google\Chrome\Application\chrome.exe"),
                os.path.expandvars(r"%ProgramFiles(x86)%\Google\Chrome\Application\chrome.exe"),
                os.path.expandvars(r"%LocalAppData%\Google\Chrome\Application\chrome.exe"),
            ]
            edge_paths = [
                os.path.expandvars(r"%ProgramFiles%\Microsoft\Edge\Application\msedge.exe"),
                os.path.expandvars(r"%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe"),
            ]
        elif system == "Darwin":  # macOS
            brave_paths = [
                "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
            ]
            chrome_paths = [
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            ]
            edge_paths = [
                "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
            ]
        elif system == "Linux":
            brave_paths = [
                "/usr/bin/brave-browser",
            ]
            chrome_paths = [
                "/usr/bin/google-chrome",
                "/usr/bin/chromium",
                "/usr/bin/chromium-browser",
            ]
            edge_paths = [
                "/usr/bin/microsoft-edge",
            ]
        else:
            raise RuntimeError(f"Unsupported operating system: {system}")

        # Check for Brave
        for path in brave_paths:
            if os.path.exists(path):
                return path, "brave"

        # Check for Chrome
        for path in chrome_paths:
            if os.path.exists(path):
                return path, "chrome"

        # Check for Edge
        for path in edge_paths:
            if os.path.exists(path):
                return path, "edge"

        return None, None

    def first_time_setup(self, log_text, headless=False):
        log_text.insert(tk.END, "Launching browser for first-time setup...\n")
        try:
            self.driver = self.setup_browser(headless=headless)
            self.driver.get("https://web.whatsapp.com")
            log_text.insert(tk.END, "Waiting for WhatsApp Web login...\n")
            WebDriverWait(self.driver, 300).until(
                EC.presence_of_element_located((By.XPATH, "//canvas[@aria-label='Scan me!']"))
            )
            log_text.insert(tk.END, "QR code loaded. Please scan with your phone.\n")
            WebDriverWait(self.driver, 300).until_not(
                EC.presence_of_element_located((By.XPATH, "//canvas[@aria-label='Scan me!']"))
            )
            log_text.insert(tk.END, "Logged into WhatsApp Web successfully.\n")
        except NoSuchWindowException:
            log_text.insert(tk.END, "Browser closed. Setup complete.\n")
        except WebDriverException as e:
            log_text.insert(tk.END, f"Unexpected error during setup: {e}\n")
        finally:
            try:
                self.driver.quit()
            except Exception:
                pass

    def send_messages(self, log_text, contacts_file, message_file, timer_min, timer_max, headless=False):
        log_text.insert(tk.END, "Starting the WhatsApp message blaster...\n")
        try:
            self.driver = self.setup_browser(headless=headless)
            self.driver.get("https://web.whatsapp.com")
            log_text.insert(tk.END, "Waiting for WhatsApp Web to load...\n")

            WebDriverWait(self.driver, 120).until(
                EC.presence_of_element_located((By.XPATH, "//div[@id='pane-side']"))
            )
            log_text.insert(tk.END, "WhatsApp Web loaded successfully.\n")

            # Load contacts
            with open(contacts_file, "r") as file:
                contacts = [line.strip() for line in file if line.strip()]

            # Load message
            with open(message_file, "r", encoding="utf-8") as file:
                message = file.read().strip()

            log_text.insert(tk.END, f"Found {len(contacts)} contacts. Sending messages...\n")
            encoded_message = urllib.parse.quote_plus(message)

            # List to store skipped contacts
            skipped_contacts = []

            for contact in contacts[:]:  # Iterate over a copy of the list
                if stop_event.is_set():
                    log_text.insert(tk.END, "Process stopped by user.\n")
                    break
                try:
                    whatsapp_url = f"https://web.whatsapp.com/send?phone={contact}&text={encoded_message}"
                    self.driver.get(whatsapp_url)
                    time.sleep(random.uniform(timer_min, timer_max))
                    try:
                        send_button = self.driver.find_element(By.XPATH, "//span[@data-icon='send']")
                        send_button.click()
                        log_text.insert(tk.END, f"Message sent to {contact}.\n")
                        # Remove the contact from the list and update the file
                        contacts.remove(contact)
                        with open(contacts_file, "w") as file:
                            file.write("\n".join(contacts))
                    except NoSuchElementException:
                        log_text.insert(tk.END, f"Number {contact} not found, retrying...\n")
                        time.sleep(5)
                        try:
                            send_button = self.driver.find_element(By.XPATH, "//span[@data-icon='send']")
                            send_button.click()
                            log_text.insert(tk.END, f"Message sent to {contact} after retry.\n")
                            # Remove the contact from the list and update the file
                            contacts.remove(contact)
                            with open(contacts_file, "w") as file:
                                file.write("\n".join(contacts))
                        except NoSuchElementException:
                            log_text.insert(tk.END, f"Skipping {contact}: Number not found.\n")
                            # Add the skipped contact to the skipped_contacts list
                            skipped_contacts.append(contact)
                            # Remove the contact from the list and update the file
                            contacts.remove(contact)
                            with open(contacts_file, "w") as file:
                                file.write("\n".join(contacts))
                    time.sleep(random.uniform(timer_min, timer_max))
                except Exception as e:
                    log_text.insert(tk.END, f"Failed to send message to {contact}: {e}\n")
                    # Add the failed contact to the skipped_contacts list
                    skipped_contacts.append(contact)
                    # Remove the contact from the list and update the file
                    contacts.remove(contact)
                    with open(contacts_file, "w") as file:
                        file.write("\n".join(contacts))

            log_text.insert(tk.END, "All messages sent. Closing browser.\n")
            self.driver.quit()

            # Log skipped contacts to a file
            if skipped_contacts:
                skipped_file = os.path.join(os.path.dirname(contacts_file), "skipped_contacts.txt")
                with open(skipped_file, "a") as file:
                    file.write("\n".join(skipped_contacts) + "\n")
                log_text.insert(tk.END, f"Skipped contacts logged to {skipped_file}\n")

        except Exception as e:
            log_text.insert(tk.END, f"Error: {e}\n")
        finally:
            stop_event.clear()

def create_gui():
    root = tk.Tk()
    root.title("WhatsApp Blaster")
    root.geometry("450x490")
    root.configure(bg="#333333")
    root.resizable(width=False, height=False)

    tk.Label(root, text="WhatsApp Blaster", bg="#333333", fg="white", font=("Arial", 20)).pack(pady=5)

    # Create a frame for the buttons
    button_frame = tk.Frame(root, bg="#333333")
    button_frame.pack(pady=10)

    contacts_file = tk.StringVar()
    message_file = tk.StringVar()
    timer_min = tk.StringVar(value=str(TIMER_MIN))
    timer_max = tk.StringVar(value=str(TIMER_MAX))
    headless_mode = tk.BooleanVar(value=False)  # Toggle for headless mode

    blaster = WhatsAppBlaster()

    def import_contacts():
        file_path = filedialog.askopenfilename(filetypes=[("Text Files", "*.txt")])
        if file_path:
            contacts_file.set(file_path)
            log_text.insert(tk.END, f"Contacts file loaded: {file_path}\n")

    def import_message():
        file_path = filedialog.askopenfilename(filetypes=[("Text Files", "*.txt")])
        if file_path:
            message_file.set(file_path)
            log_text.insert(tk.END, f"Message file loaded: {file_path}\n")

    def first_time_setup_wrapper():
        threading.Thread(target=blaster.first_time_setup, args=(log_text, headless_mode.get())).start()

    def send_messages_wrapper():
        if not contacts_file.get():
            messagebox.showerror("Error", "Please load a contacts file first.")
            return
        if not message_file.get():
            messagebox.showerror("Error", "Please load a message file first.")
            return
        try:
            timer_min_val = float(timer_min.get())
            timer_max_val = float(timer_max.get())
            if timer_min_val >= timer_max_val or timer_min_val < 0 or timer_max_val < 0:
                raise ValueError("Invalid timer values.")
        except ValueError:
            messagebox.showerror("Error", "Please enter valid numeric timer values.")
            return
        threading.Thread(target=blaster.send_messages, args=(log_text, contacts_file.get(), message_file.get(), timer_min_val, timer_max_val, headless_mode.get())).start()

    def stop_process():
        stop_event.set()
        log_text.insert(tk.END, "Stopping process...\n")

    # Arrange buttons in 2 rows and 3 columns
    ttk.Button(button_frame, text="1) Import Contacts", width=20, command=import_contacts).grid(row=0, column=0, padx=5, pady=5)
    ttk.Button(button_frame, text="2) Import Message", width=20, command=import_message).grid(row=0, column=1, padx=5, pady=5)
    ttk.Button(button_frame, text="3) Launch WA Web", width=20, command=first_time_setup_wrapper).grid(row=0, column=2, padx=5, pady=5)
    ttk.Button(button_frame, text="RUN", width=20, command=send_messages_wrapper).grid(row=1, column=0, padx=5, pady=5)
    ttk.Button(button_frame, text="STOP", width=20, command=stop_process).grid(row=1, column=1, padx=5, pady=5)
    # ttk.Checkbutton(button_frame, text="Headless Mode", variable=headless_mode).grid(row=1, column=2, padx=5, pady=5)

    # Headless mode button logic
    def toggle_headless():
        if headless_mode.get():
            headless_mode.set(False)
            headless_button.config(text="Not Headless", style="TButton")
        else:
            headless_mode.set(True)
            headless_button.config(text="Headless", style="TButton")

    # Create Headless Mode Button
    headless_button = ttk.Button(button_frame, text="Not Headless", command=toggle_headless, width=20)
    headless_button.grid(row=1, column=2, padx=5, pady=5)

    # Timer frame
    timer_frame = tk.Frame(root, bg="#333333")
    timer_frame.pack(pady=10)
    tk.Label(timer_frame, text="Min Timer (sec):", bg="#333333", fg="white").pack(side=tk.LEFT, padx=5)
    tk.Entry(timer_frame, textvariable=timer_min, width=5).pack(side=tk.LEFT, padx=5)
    tk.Label(timer_frame, text="Max Timer (sec):", bg="#333333", fg="white").pack(side=tk.LEFT, padx=5)
    tk.Entry(timer_frame, textvariable=timer_max, width=5).pack(side=tk.LEFT, padx=5)

    # Logs
    tk.Label(root, text="Logs", bg="#333333", fg="white", font=("Arial", 12)).pack(anchor="w", padx=10)
    log_text = scrolledtext.ScrolledText(root, width=70, height=15, font=("Arial", 10))
    log_text.pack(padx=10, pady=10)

    tk.Label(root, text="qt3000@hw.ac.uk", bg="#333333", fg="white", font=("Arial", 10)).pack(side=tk.BOTTOM, pady=1)

    root.mainloop()

if __name__ == "__main__":
    create_gui()
