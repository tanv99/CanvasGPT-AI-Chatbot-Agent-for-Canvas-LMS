# import requests

# def test_canvas_connection():
#     try:
#         headers = {
#             'Authorization': f'Bearer {API_KEY}',
#             'Content-Type': 'application/json'
#         }
        
#         # Get courses first
#         courses_response = requests.get(f'{API_URL}/courses', headers=headers)
        
#         if courses_response.status_code == 200:
#             courses = courses_response.json()
#             if isinstance(courses, list) and courses:
#                 course_id = courses[0]['id']  # Using first course
                
#                 # Post announcement
#                 announcement_data = {
#                     'title': 'Welcome!',
#                     'message': 'Hi everyone! Welcome to the course.',
#                     'is_announcement': True
#                 }
                
#                 announcement_response = requests.post(
#                     f'{API_URL}/courses/{course_id}/discussion_topics',
#                     headers=headers,
#                     json=announcement_data
#                 )
                
#                 print(f"Announcement Status: {announcement_response.status_code}")
#                 print(announcement_response.text)
        
#     except requests.RequestException as e:
#         print(f"Error: {e}")

# if __name__ == '__main__':
#     test_canvas_connection()

#########################################################################################################################
import requests
import os
import json
from dotenv import load_dotenv
load_dotenv()
API_URL = os.getenv('API_URL')
API_KEY = os.getenv('API_KEY')
PDF_PATH = os.getenv('PDF_PATH')

def test_canvas_connection():
    try:
        headers = {
            'Authorization': f'Bearer {API_KEY}'
        }
        
        # Get courses
        courses_response = requests.get(f'{API_URL}/courses', headers=headers)
        
        if courses_response.status_code == 200:
            courses = courses_response.json()
            if isinstance(courses, list) and courses:
                course_id = courses[0]['id']
                
                # Upload file first
                file_params = {
                    'name': os.path.basename(PDF_PATH),
                    'content_type': 'application/pdf',
                    'parent_folder_path': '/',
                    'size': os.path.getsize(PDF_PATH)
                }
                
                headers['Content-Type'] = 'application/json'
                init_upload = requests.post(
                    f'{API_URL}/courses/{course_id}/files',
                    headers=headers,
                    data=json.dumps(file_params)
                )
                
                if init_upload.status_code == 200:
                    upload_data = init_upload.json()
                    upload_url = upload_data['upload_url']
                    upload_params = upload_data['upload_params']
                    
                    with open(PDF_PATH, 'rb') as pdf_file:
                        files = {
                            'file': (os.path.basename(PDF_PATH), pdf_file, 'application/pdf')
                        }
                        upload_response = requests.post(
                            upload_url,
                            data=upload_params,
                            files=files
                        )
                    
                    if upload_response.status_code == 201:
                        file_info = upload_response.json()
                        file_id = file_info['id']
                        file_url = file_info['url']
                        preview_url = file_info.get('preview_url', '')
                        
                        # Create announcement with HTML link
                        message_html = f'''
                        <p>Hi everyone! Welcome to the course.</p>
                        <p>Please find the attached PDF: <a href="{file_url}" target="_blank" rel="noopener noreferrer">Click here to download PDF</a></p>
                        '''
                        
                        announcement_data = {
                            'title': 'Welcome!',
                            'message': message_html,
                            'is_announcement': True,
                            'published': True,
                            'attachment_ids[]': [file_id],
                            'file_ids[]': [file_id]
                        }
                        
                        announcement_response = requests.post(
                            f'{API_URL}/courses/{course_id}/discussion_topics',
                            headers=headers,
                            data=json.dumps(announcement_data)
                        )
                        
                        print(f"Announcement Status: {announcement_response.status_code}")
                        print(f"Announcement Response: {announcement_response.text}")
                        
                        if announcement_response.status_code != 200:
                            # Try alternative format
                            message_html = f'''
                            <div>Hi everyone! Welcome to the course.</div>
                            <div><a class="instructure_file_link" href="/courses/{course_id}/files/{file_id}/download">
                            Click here to download PDF</a></div>
                            '''
                            announcement_data['message'] = message_html
                            
                            announcement_response = requests.post(
                                f'{API_URL}/courses/{course_id}/discussion_topics',
                                headers=headers,
                                data=json.dumps(announcement_data)
                            )
                            print(f"Second attempt status: {announcement_response.status_code}")
                            
                    else:
                        print(f"File upload failed: {upload_response.text}")
                else:
                    print(f"Init upload failed: {init_upload.text}")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise e

if __name__ == '__main__':
    test_canvas_connection()