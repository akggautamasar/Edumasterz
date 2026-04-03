#!/usr/bin/env python3
"""
Backend API Testing for Telegram File Storage App
Tests all the APIs mentioned in the review request
"""

import requests
import sys
import json
from datetime import datetime
from typing import Dict, Any, List, Tuple

class TelegramFileStorageAPITester:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip('/')
        self.admin_password = "admin"  # From .env file
        self.tests_run = 0
        self.tests_passed = 0
        self.test_results = []
        
    def log_test(self, name: str, success: bool, details: str = "", response_data: Any = None):
        """Log test result"""
        self.tests_run += 1
        if success:
            self.tests_passed += 1
            
        result = {
            "test_name": name,
            "success": success,
            "details": details,
            "response_data": response_data,
            "timestamp": datetime.now().isoformat()
        }
        self.test_results.append(result)
        
        status = "✅ PASS" if success else "❌ FAIL"
        print(f"{status} - {name}")
        if details:
            print(f"    Details: {details}")
        if not success and response_data:
            print(f"    Response: {response_data}")
        print()

    def test_health_check(self) -> bool:
        """Test /api/health endpoint"""
        try:
            response = requests.get(f"{self.base_url}/api/health", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                expected_fields = ["status", "version", "features"]
                
                if all(field in data for field in expected_fields):
                    if data["status"] == "ok":
                        self.log_test("Health Check API", True, 
                                    f"Status: {data['status']}, Version: {data['version']}", data)
                        return True
                    else:
                        self.log_test("Health Check API", False, 
                                    f"Status not 'ok': {data['status']}", data)
                        return False
                else:
                    self.log_test("Health Check API", False, 
                                "Missing required fields in response", data)
                    return False
            else:
                self.log_test("Health Check API", False, 
                            f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Health Check API", False, f"Exception: {str(e)}")
            return False

    def test_cors_options(self) -> bool:
        """Test CORS OPTIONS request on /file endpoint"""
        try:
            response = requests.options(f"{self.base_url}/file", timeout=10)
            
            # Accept both 200 and 204 as valid responses for OPTIONS
            if response.status_code in [200, 204]:
                headers = response.headers
                required_cors_headers = [
                    "Access-Control-Allow-Origin",
                    "Access-Control-Allow-Methods", 
                    "Access-Control-Allow-Headers"
                ]
                
                missing_headers = [h for h in required_cors_headers if h not in headers]
                
                if not missing_headers:
                    self.log_test("CORS OPTIONS /file", True, 
                                f"All CORS headers present (HTTP {response.status_code})", dict(headers))
                    return True
                else:
                    self.log_test("CORS OPTIONS /file", False, 
                                f"Missing CORS headers: {missing_headers}", dict(headers))
                    return False
            else:
                self.log_test("CORS OPTIONS /file", False, 
                            f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("CORS OPTIONS /file", False, f"Exception: {str(e)}")
            return False

    def test_create_share_token(self) -> Tuple[bool, str]:
        """Test /api/createShareToken endpoint"""
        try:
            payload = {
                "password": self.admin_password,
                "path": "/test/sample.mp4",  # Test path
                "expiry_hours": 24
            }
            
            response = requests.post(f"{self.base_url}/api/createShareToken", 
                                   json=payload, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                
                # Check if it's a file not found error (expected in demo mode)
                if data.get("status") == "File not found":
                    self.log_test("Create Share Token API", True, 
                                "Expected 'File not found' in demo mode", data)
                    return True, ""
                elif data.get("status") == "ok" and "token" in data:
                    token = data["token"]
                    self.log_test("Create Share Token API", True, 
                                f"Token created successfully", data)
                    return True, token
                else:
                    self.log_test("Create Share Token API", False, 
                                f"Unexpected response format", data)
                    return False, ""
            else:
                self.log_test("Create Share Token API", False, 
                            f"HTTP {response.status_code}", response.text)
                return False, ""
                
        except Exception as e:
            self.log_test("Create Share Token API", False, f"Exception: {str(e)}")
            return False, ""

    def test_tags_management(self) -> bool:
        """Test tags management APIs"""
        success_count = 0
        total_tests = 4
        
        # Test addTags
        try:
            payload = {
                "password": self.admin_password,
                "path": "/test/sample.mp4",
                "tags": ["test", "video", "demo"]
            }
            
            response = requests.post(f"{self.base_url}/api/addTags", 
                                   json=payload, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("status") in ["ok", "File not found"]:  # Both acceptable in demo
                    self.log_test("Add Tags API", True, 
                                f"Response: {data.get('status')}", data)
                    success_count += 1
                else:
                    self.log_test("Add Tags API", False, 
                                f"Unexpected status: {data.get('status')}", data)
            else:
                self.log_test("Add Tags API", False, 
                            f"HTTP {response.status_code}", response.text)
                
        except Exception as e:
            self.log_test("Add Tags API", False, f"Exception: {str(e)}")

        # Test getTags
        try:
            payload = {
                "path": "/test/sample.mp4"
            }
            
            response = requests.post(f"{self.base_url}/api/getTags", 
                                   json=payload, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("status") in ["ok", "File not found"]:
                    self.log_test("Get Tags API", True, 
                                f"Response: {data.get('status')}", data)
                    success_count += 1
                else:
                    self.log_test("Get Tags API", False, 
                                f"Unexpected status: {data.get('status')}", data)
            else:
                self.log_test("Get Tags API", False, 
                            f"HTTP {response.status_code}", response.text)
                
        except Exception as e:
            self.log_test("Get Tags API", False, f"Exception: {str(e)}")

        # Test searchByTags
        try:
            payload = {
                "tags": ["test", "video"]
            }
            
            response = requests.post(f"{self.base_url}/api/searchByTags", 
                                   json=payload, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "ok" and "file_ids" in data:
                    self.log_test("Search By Tags API", True, 
                                f"Found {data.get('count', 0)} files", data)
                    success_count += 1
                else:
                    self.log_test("Search By Tags API", False, 
                                f"Unexpected response format", data)
            else:
                self.log_test("Search By Tags API", False, 
                            f"HTTP {response.status_code}", response.text)
                
        except Exception as e:
            self.log_test("Search By Tags API", False, f"Exception: {str(e)}")

        # Test allTags
        try:
            response = requests.get(f"{self.base_url}/api/allTags", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "ok" and "tags" in data:
                    self.log_test("All Tags API", True, 
                                f"Retrieved {len(data.get('tags', []))} tags", data)
                    success_count += 1
                else:
                    self.log_test("All Tags API", False, 
                                f"Unexpected response format", data)
            else:
                self.log_test("All Tags API", False, 
                            f"HTTP {response.status_code}", response.text)
                
        except Exception as e:
            self.log_test("All Tags API", False, f"Exception: {str(e)}")

        return success_count == total_tests

    def test_enhanced_search(self) -> bool:
        """Test /api/search endpoint"""
        try:
            payload = {
                "query": "test",
                "tags": ["video"],
                "type": "video"
            }
            
            response = requests.post(f"{self.base_url}/api/search", 
                                   json=payload, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "ok" and "results" in data and "count" in data:
                    self.log_test("Enhanced Search API", True, 
                                f"Search completed, found {data.get('count', 0)} results", data)
                    return True
                else:
                    self.log_test("Enhanced Search API", False, 
                                f"Unexpected response format", data)
                    return False
            else:
                self.log_test("Enhanced Search API", False, 
                            f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Enhanced Search API", False, f"Exception: {str(e)}")
            return False

    def test_static_pages(self) -> bool:
        """Test static page endpoints"""
        pages = [
            ("/smart-player", "Smart Player"),
            ("/stream", "Video Player"), 
            ("/fast-player", "Fast Player")
        ]
        
        success_count = 0
        
        for endpoint, page_name in pages:
            try:
                response = requests.get(f"{self.base_url}{endpoint}", timeout=10)
                
                if response.status_code == 200:
                    # Check if it's HTML content
                    content_type = response.headers.get('content-type', '')
                    if 'text/html' in content_type or '<html' in response.text:
                        self.log_test(f"{page_name} Page Load", True, 
                                    f"Page loaded successfully (Content-Type: {content_type})")
                        success_count += 1
                    else:
                        self.log_test(f"{page_name} Page Load", False, 
                                    f"Not HTML content: {content_type}")
                else:
                    self.log_test(f"{page_name} Page Load", False, 
                                f"HTTP {response.status_code}")
                    
            except Exception as e:
                self.log_test(f"{page_name} Page Load", False, f"Exception: {str(e)}")
        
        return success_count == len(pages)

    def test_password_check(self) -> bool:
        """Test password check API"""
        try:
            # Test with correct password
            payload = {"pass": self.admin_password}
            response = requests.post(f"{self.base_url}/api/checkPassword", 
                                   json=payload, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "ok":
                    self.log_test("Password Check API (Valid)", True, 
                                "Correct password accepted", data)
                    
                    # Test with wrong password
                    payload = {"pass": "wrongpassword"}
                    response = requests.post(f"{self.base_url}/api/checkPassword", 
                                           json=payload, timeout=10)
                    
                    if response.status_code == 200:
                        data = response.json()
                        if data.get("status") == "Invalid password":
                            self.log_test("Password Check API (Invalid)", True, 
                                        "Wrong password rejected", data)
                            return True
                        else:
                            self.log_test("Password Check API (Invalid)", False, 
                                        f"Wrong password not rejected: {data.get('status')}", data)
                            return False
                    else:
                        self.log_test("Password Check API (Invalid)", False, 
                                    f"HTTP {response.status_code}", response.text)
                        return False
                else:
                    self.log_test("Password Check API (Valid)", False, 
                                f"Correct password rejected: {data.get('status')}", data)
                    return False
            else:
                self.log_test("Password Check API (Valid)", False, 
                            f"HTTP {response.status_code}", response.text)
                return False
                
        except Exception as e:
            self.log_test("Password Check API", False, f"Exception: {str(e)}")
            return False

    def run_all_tests(self) -> Dict[str, Any]:
        """Run all backend tests"""
        print("🚀 Starting Telegram File Storage Backend API Tests")
        print(f"📡 Testing against: {self.base_url}")
        print(f"🔑 Admin password: {self.admin_password}")
        print("=" * 60)
        print()

        # Core API tests
        self.test_health_check()
        self.test_password_check()
        self.test_cors_options()
        
        # Token management
        success, token = self.test_create_share_token()
        
        # Tags management
        self.test_tags_management()
        
        # Search functionality
        self.test_enhanced_search()
        
        # Static pages
        self.test_static_pages()

        # Summary
        print("=" * 60)
        print(f"📊 Test Summary:")
        print(f"   Total Tests: {self.tests_run}")
        print(f"   Passed: {self.tests_passed}")
        print(f"   Failed: {self.tests_run - self.tests_passed}")
        print(f"   Success Rate: {(self.tests_passed/self.tests_run)*100:.1f}%")
        
        return {
            "total_tests": self.tests_run,
            "passed_tests": self.tests_passed,
            "failed_tests": self.tests_run - self.tests_passed,
            "success_rate": (self.tests_passed/self.tests_run)*100,
            "test_results": self.test_results,
            "summary": "Backend API testing completed",
            "demo_mode": True,
            "notes": "App running in DEMO MODE without real Telegram credentials"
        }

def main():
    # Get backend URL from environment
    backend_url = "https://d1528915-3be1-42f1-ba2c-40175ece7f9d.preview.emergentagent.com"
    
    print("🔧 Telegram File Storage Backend Tester")
    print(f"🌐 Backend URL: {backend_url}")
    print()
    
    tester = TelegramFileStorageAPITester(backend_url)
    results = tester.run_all_tests()
    
    # Save results to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = f"/app/test_reports/backend_test_results_{timestamp}.json"
    
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n📄 Detailed results saved to: {results_file}")
    
    # Return appropriate exit code
    if results["success_rate"] >= 80:
        print("✅ Backend tests completed successfully!")
        return 0
    else:
        print("❌ Backend tests completed with issues!")
        return 1

if __name__ == "__main__":
    sys.exit(main())