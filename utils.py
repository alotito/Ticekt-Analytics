import json
import re
from typing import List, Dict # UPDATED: Added Dict

def parse_llm_output(raw_output: str) -> List[str]:
    """
    Tries to parse the JSON string from the LLM.
    Uses regex to find the JSON blob within potentially messy output.
    """
    try:
        match = re.search(r'\{.*\}', raw_output, re.DOTALL)
        
        if not match:
            print(f"Warning: No JSON object found in LLM output: {raw_output}")
            return []

        json_string = match.group(0)
        data = json.loads(json_string)
        skills = data.get('skills', [])
        
        return skills if isinstance(skills, list) else []
            
    except json.JSONDecodeError:
        print(f"Warning: Failed to decode JSON from extracted string: {json_string}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred during parsing: {e}")
        return []

def parse_meta_analysis_output(raw_output: str) -> List[Dict]:
    """
    Tries to parse the JSON array from the meta-analysis LLM call.
    Uses regex to find the JSON blob within potentially messy output.
    """
    try:
        match = re.search(r'\[.*\]', raw_output, re.DOTALL)
        
        if not match:
            print(f"Warning: No JSON array found in LLM output: {raw_output}")
            return []

        json_string = match.group(0)
        data = json.loads(json_string)
        
        return data if isinstance(data, list) else []
            
    except json.JSONDecodeError:
        print(f"Warning: Failed to decode JSON from extracted string: {json_string}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred during meta-analysis parsing: {e}")
        return []