# analytics_engine/llm_interface.py (Corrected Version)
import requests
from typing import List
from abc import ABC, abstractmethod

# The "contract" - note the return type is now str
class LLMInterface(ABC):
    @abstractmethod
    def get_skill_analysis(self, ticket_text: str) -> str:
        pass

class OllamaInterface(LLMInterface):
    def __init__(self, model_name: str, prompt_template_path: str, host: str = "http://localhost:11434"):
        self.model_name = model_name
        self.api_url = f"{host}/api/generate"  # Correct variable is defined here
        try:
            with open(prompt_template_path, 'r') as f:
                self.prompt_template = f.read()
            print(f"Successfully loaded prompt template from: {prompt_template_path}")
        except FileNotFoundError:
            print(f"ERROR: Prompt file not found at '{prompt_template_path}'.")
            self.prompt_template = "Format your response as JSON. Extract skills from this text: {ticket_text}"

    def get_skill_analysis(self, ticket_text: str) -> str:
        prompt = self.prompt_template.format(ticket_text=ticket_text)

        try:
            payload = {
                "model": self.model_name,
                "prompt": prompt,
                "stream": False
            }
            # CORRECTED: Changed self.api_g to self.api_url
            response = requests.post(self.api_url, json=payload, timeout=90)
            response.raise_for_status()
            
            content_string = response.json().get('response', '').strip()
            return content_string

        except requests.exceptions.RequestException as e:
            print(f"Error communicating with Ollama: {e}")
            return f"Error: Could not communicate with Ollama. {e}"
        except Exception as e:
            print(f"An unknown error occurred: {e}")
            return f"Error: An unknown error occurred. {e}"