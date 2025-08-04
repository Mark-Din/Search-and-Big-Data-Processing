from fastapi import FastAPI, APIRouter, HTTPException, Depends, Query, status
from connection import ElasticSearchConnectionManager
from typing import Any, Dict
from common import initlog

logger = initlog('analysis')

router = APIRouter(prefix='/analysis', tags=["analysis"])

TEACHER_REGEX = "^ESG_使用者_教師"
STUDENT_REGEX = "^ESG_使用者_學生"
PRODUCT_REGEX = "^使用者_商品"
SALES_REGEX = "^使用者_銷售"

# Get project data, format and return as a dictionary
def get_project_dict(project: Dict[str, Any], uid: int, regex_pattern: str) -> Dict[str, Any]:
    logger.info(f"Processing project data for project: {project['_id']}")

    try:
        keys_to_include = ['uid', '賣家id', '課程講師編號', '使用者id','賣家uid'] 

        project_dict = {
            "_id": str(project["_id"]),
            "project_name": project["note"],
            "dict": {
                data["_connectionInfo"]: {
                    key: uid
                    for key in data["_columnArray"].keys()
                    if key in keys_to_include
                }
                for data in project["data"]
            }
        }
        return project_dict
    except KeyError as e:
        logger.error(f"Key error: {e}")
        raise HTTPException(status_code=500, detail="Failed to process project data.")

# Fetch projects from MongoDB
def fetch_projects(uid: int, regex_pattern: str) -> Dict[str, Any]:
    try:
        collection = ElasticSearchConnectionManager.create_mongo_connection()
        # Use count_documents() instead of projects.count()
        project_count = collection.count_documents({"note": {"$regex": regex_pattern}})  # Count based on the same filter
        logger.info(f"Found {project_count} projects.")

        projects = collection.find({"note": {"$regex": regex_pattern}})
        
        results = {}
        for project in projects:
            results = get_project_dict(project, uid, regex_pattern)

        if not results:
            raise HTTPException(status_code=404, detail="No projects found.")
        
        return results['dict']
    
    except Exception as e:
        logger.error(f"Failed to retrieve projects: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve projects.")


# Get projects for a teacher or student
@router.get("/teacher")
def get_teacher_projects(uid: int = Query(None)):
    return fetch_projects(uid, TEACHER_REGEX)

@router.get("/student")
def get_student_projects(uid: int = Query(None)):
    return fetch_projects(uid, STUDENT_REGEX)

@router.get("/allgets_products")
def get_products_project(uid: int = Query(None)):
    return fetch_projects(uid, PRODUCT_REGEX)

@router.get("/allgets_sales")
def get_sales_projects(uid: int = Query(None)):
    return fetch_projects(uid, SALES_REGEX)