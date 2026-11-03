"""Observed GitHub updated-since issue cursor protocol, with strict context guards.

Provider Link receipts are the authority. This encoder is not a promise that
GitHub's opaque protocol will remain stable. Tied timestamps do not imply sorted
issue IDs: a real provider boundary resumed with a lower ID at the same second.
"""
import base64
from datetime import datetime,timezone
import struct
from urllib.parse import urlparse,parse_qs

def uint(value):
    if not isinstance(value,int) or value<0:raise ValueError('Nonnegative integer required')
    if value<=127:return bytes([value])
    if value<=255:return b'\xcc'+struct.pack('>B',value)
    if value<=65535:return b'\xcd'+struct.pack('>H',value)
    if value<=4294967295:return b'\xce'+struct.pack('>I',value)
    return b'\xcf'+struct.pack('>Q',value)

def cursor(updated_at,issue_id):
    value=datetime.fromisoformat(updated_at.replace('Z','+00:00'))
    if value.tzinfo is None:raise ValueError('Timestamp zone required')
    millis=int(value.timestamp()*1000)
    return base64.b64encode(b'cursor:v2:'+b'\x92'+uint(millis)+uint(issue_id)).decode()

def validate_page(rows,anchor=None,since='2022-12-31T23:59:59Z'):
    if not isinstance(rows,list) or len(rows)>100:raise ValueError('Bounded issue array required')
    ids=set();previous=anchor
    for row in rows:
        if row['id'] in ids:raise ValueError('Duplicate ID inside page')
        ids.add(row['id'])
        if row['updated_at']<=since:raise ValueError('Update boundary violated')
        if previous and row['updated_at']<previous['updated_at']:raise ValueError('Update timestamp decreased')
        if anchor and row['id']==anchor['id'] and row['updated_at']==anchor['updated_at']:
            raise ValueError('Identical prior boundary repeated')
        previous=row
    return {'terminal_page':not rows,'last_anchor':previous,'cursor_out':cursor(previous['updated_at'],previous['id']) if previous else None}

def check_context(url,repo,since,per_page=100):
    parsed=urlparse(url);query=parse_qs(parsed.query)
    if parsed.scheme!='https' or parsed.netloc!='api.github.com' or parsed.path!='/repos/'+repo+'/issues':raise ValueError('Frozen endpoint changed')
    expected={'state':['all'],'sort':['updated'],'direction':['asc'],'since':[since],'per_page':[str(per_page)]}
    if any(query.get(k)!=v for k,v in expected.items()):raise ValueError('Cursor request filters changed')
    if set(query)-set(expected)-{'after'}:raise ValueError('Unexpected cursor filter')
    if any(len(v)!=1 for v in query.values()):raise ValueError('Repeated cursor filter')
    return True
