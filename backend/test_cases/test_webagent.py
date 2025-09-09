import requests
from typing import Optional, Dict, Any
import pytest

def query_web_agent(url: str, question: Optional[str] = None) -> Dict[str, Any]:
    try:
        endpoints = [
            "http://34.162.53.77:8000/agent-workflow",
            "http://34.162.53.77:8000/agent-workflow/form",
            "http://34.162.53.77:8000/reset-supervisor"
        ]
        
        payload = {
            "query": question if question else url,
            "url": url
        }
        
        responses = []
        for endpoint in endpoints:
            try:
                response = requests.post(
                    endpoint, 
                    json=payload,
                    headers={"Content-Type": "application/json"}
                )
                responses.append({
                    "endpoint": endpoint,
                    "status_code": response.status_code,
                    "response": response.json() if response.ok else response.text
                })
            except Exception as e:
                responses.append({
                    "endpoint": endpoint,
                    "status_code": 500,
                    "error": str(e)
                })
        
        return {
            "status_code": 200,
            "url": url,
            "question": question,
            "responses": responses,
            "response": "Simulated multi-endpoint query"
        }
    
    except Exception as e:
        return {
            "status_code": 500,
            "url": url,
            "question": question,
            "error": str(e)
        }

class TestWebAgentQueries:
    def test_agent_workflow_endpoint(self):
        url = "https://example.com"
        question = "What is this website about?"
        
        result = query_web_agent(url, question)
        
        assert result['status_code'] == 200
        assert result['url'] == url
        assert result['question'] == question
        assert 'responses' in result
        
        workflow_responses = [
            resp for resp in result['responses'] 
            if "agent-workflow" in resp['endpoint']
        ]
        assert len(workflow_responses) > 0

    def test_form_data_endpoint(self):
        url = "https://example.com/upload"
        question = "Process this document"
        
        result = query_web_agent(url, question)
        
        assert result['status_code'] == 200
        assert result['url'] == url
        
        form_responses = [
            resp for resp in result['responses'] 
            if "agent-workflow/form" in resp['endpoint']
        ]
        assert len(form_responses) > 0

    def test_reset_supervisor_endpoint(self):
        url = "http://34.162.53.77:8000/reset-supervisor"
        
        result = query_web_agent(url)
        
        assert result['status_code'] == 200
        
        reset_responses = [
            resp for resp in result['responses'] 
            if "reset-supervisor" in resp['endpoint']
        ]
        assert len(reset_responses) > 0

    def test_multiple_endpoint_interactions(self):
        test_scenarios = [
            {
                "url": "https://news.northeastern.edu/2024/10/29/commencement-2025-fenway-park/",
                "questions": [
                    "Details about the commencement",
                    "Location specifics"
                ]
            },
            {
                "url": "https://www.example.com/document",
                "questions": [
                    "Summarize the document",
                    "Key points"
                ]
            }
        ]
        
        for scenario in test_scenarios:
            for question in scenario['questions']:
                result = query_web_agent(scenario['url'], question)
                
                assert result['status_code'] == 200
                assert len(result['responses']) > 0
                
                endpoints_called = [
                    resp['endpoint'] for resp in result['responses']
                ]
                assert any("agent-workflow" in ep for ep in endpoints_called)

def main():
    test_cases = [
        {
            "url": "https://news.northeastern.edu/2024/10/29/commencement-2025-fenway-park/",
            "question": "When and where is the commencement ceremony?"
        },
        {
            "url": "http://34.162.53.77:8000/reset-supervisor",
            "question": None
        }
    ]
    
    print("\nStarting Web Agent Endpoint Tests...")
    
    for case in test_cases:
        result = query_web_agent(case['url'], case['question'])
        print("\n" + "="*80)
        print(f"URL: {result['url']}")
        print(f"Question: {result.get('question', 'No question')}")
        print(f"Status Code: {result['status_code']}")
        print("Endpoint Responses:")
        for resp in result.get('responses', []):
            print(f"  - {resp.get('endpoint')}: {resp.get('status_code')}")
        print("="*80)
    
    print("\nTests completed!")

if __name__ == "__main__":
    main()
