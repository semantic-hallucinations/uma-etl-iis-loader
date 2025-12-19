from datetime import datetime, date
from sqlalchemy import insert, delete, select, update, func, text, and_
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession
from loguru import logger

from .models import (
    Faculty, Department, Speciality, StudentGroup, Employee, DepartmentEmployee, Auditory, 
    ScheduleJsonStorage, ScheduleEvent, SystemState,
)
from .client import BsuirApiClient

DAYS_MAP = {
    "Понедельник": 1, "Вторник": 2, "Среда": 3, "Четверг": 4, 
    "Пятница": 5, "Суббота": 6, "Воскресенье": 7
}

def _parse_weeks(weeks_list: list) -> list:
    if not weeks_list or weeks_list == [0]:
        return [1, 2, 3, 4]
    return weeks_list

def _extract_aud_names(auds: list) -> list:
    if not auds: return []
    res = []
    for a in auds:
        if isinstance(a, dict):
            val = a.get('name') or str(a.get('id', ''))
            if val: res.append(val)
        elif isinstance(a, str):
            res.append(a)
        elif isinstance(a, int):
            res.append(str(a))
    return res

def _extract_names_safe(items: list, key='name') -> list:
    if not items: return []
    res = []
    for i in items:
        if isinstance(i, dict):
            if key == 'fio':
                last = i.get('lastName', '')
                first = i.get('firstName', '')
                val = f"{last} {first}".strip()
            else:
                val = i.get(key)
            if val: res.append(str(val))
        elif isinstance(i, str):
            res.append(i)
    return res

def _parse_date(date_str: str) -> date:
    try:
        return datetime.strptime(date_str, "%d.%m.%Y").date()
    except (ValueError, TypeError):
        return None

async def sync_system_state(session: AsyncSession, client: BsuirApiClient):
    logger.info("Обновление system_state (current_week)...")
    try:
        week = await client.get_current_week()
        if week:
            stmt = pg_insert(SystemState).values(
                key="current_week", value=str(week)
            ).on_conflict_do_update(
                index_elements=['key'], set_=dict(value=str(week), updated_at=func.now())
            )
            await session.execute(stmt)
            await session.commit()
    except Exception as e:
        logger.error(f"Не удалось получить текущую неделю: {e}")

async def sync_faculties(session: AsyncSession, client: BsuirApiClient):
    logger.info("Синхронизация факультетов...")
    data = await client.get_faculties()
    for item in data:
        stmt = pg_insert(Faculty).values(
            id=item['id'], name=item['name'], abbr=item['abbrev']
        ).on_conflict_do_update(
            index_elements=['id'], set_=dict(name=item['name'], abbr=item['abbrev'])
        )
        await session.execute(stmt)
    await session.commit()

async def sync_departments(session: AsyncSession, client: BsuirApiClient):
    logger.info("Синхронизация кафедр...")
    data = await client.get_departments()
    for item in data:
        name = item.get('name') or item.get('nameAbbrev')
        stmt = pg_insert(Department).values(
            id=item['id'], name=name, abbr=item.get('abbrev') or name[:50],
            url_id=str(item.get('id'))
        ).on_conflict_do_update(
            index_elements=['id'], set_=dict(name=name, abbr=item.get('abbrev') or name[:50])
        )
        await session.execute(stmt)
    await session.commit()

async def sync_specialities(session: AsyncSession, client: BsuirApiClient):
    logger.info("Синхронизация специальностей...")
    data = await client.get_specialities()
    res = await session.execute(select(Faculty.id))
    existing_faculty_ids = set(res.scalars().all())

    for item in data:
        fac_id = item['facultyId']
        if fac_id not in existing_faculty_ids:
            session.add(Faculty(id=fac_id, name=f"Unknown Faculty {fac_id}", abbr=f"UNK-{fac_id}"))
            await session.flush()
            existing_faculty_ids.add(fac_id)

        edu_form = item.get('educationForm') or {}
        edu_form_val = str(edu_form.get('name') or edu_form.get('id') or 'Unknown')
        
        stmt = pg_insert(Speciality).values(
            id=item['id'], name=item['name'], abbr=item['abbrev'], code=item['code'],
            education_form=edu_form_val, faculty_id=fac_id
        ).on_conflict_do_update(
            index_elements=['id'],
            set_=dict(name=item['name'], abbr=item['abbrev'], code=item['code'], faculty_id=fac_id)
        )
        await session.execute(stmt)
    await session.commit()

async def sync_groups(session: AsyncSession, client: BsuirApiClient):
    logger.info("Синхронизация групп (SCD Type 2)...")
    api_groups = await client.get_student_groups()
    if not api_groups: return

    res_spec = await session.execute(select(Speciality.id))
    valid_spec_ids = set(res_spec.scalars().all())

    stmt = select(StudentGroup).where(StudentGroup.valid_to.is_(None))
    result = await session.execute(stmt)
    db_map = {g.id: g for g in result.scalars().all()}

    for item in api_groups:
        api_id = item['id']
        spec_id = item.get('specialityDepartmentEducationFormId')
        if spec_id not in valid_spec_ids: continue

        new_data = {
            'name': item['name'], 'course': item.get('course'),
            'calendar_id': item.get('calendarId'), 'education_degree': item.get('educationDegree', 1),
            'number_of_students': item.get('numberOfStudents'), 'specialty_id': spec_id
        }

        current = db_map.get(api_id)
        if not current:
            session.add(StudentGroup(id=api_id, **new_data))
        else:
            is_changed = (
                current.name != new_data['name'] or 
                current.course != new_data['course'] or
                current.specialty_id != new_data['specialty_id']
            )
            if is_changed:
                current.valid_to = datetime.now()
                session.add(current)
                session.add(StudentGroup(id=api_id, **new_data))
            elif current.number_of_students != new_data['number_of_students']:
                 current.number_of_students = new_data['number_of_students']
                 session.add(current)

    api_ids = {i['id'] for i in api_groups if i.get('specialityDepartmentEducationFormId') in valid_spec_ids}
    for db_id, db_obj in db_map.items():
        if db_id not in api_ids:
            db_obj.valid_to = datetime.now()
            session.add(db_obj)

    await session.commit()

async def sync_employees(session: AsyncSession, client: BsuirApiClient):
    logger.info("Синхронизация сотрудников...")
    data = await client.get_employees()
    
    d_res = await session.execute(select(Department.id, Department.name, Department.abbr))
    dept_map = {}
    for did, name, abbr in d_res.all():
        if name: dept_map[name.strip().lower()] = did
        if abbr: dept_map[abbr.strip().lower()] = did

    for item in data:
        if not item.get('urlId'): continue 

        stmt_emp = pg_insert(Employee).values(
            id=item['id'], first_name=item['firstName'], last_name=item['lastName'],
            middle_name=item.get('middleName'), degree=item.get('degree'),
            rank=item.get('rank'), photo_link=item.get('photoLink'),
            calendar_id=item.get('calendarId'), url_id=item['urlId']
        ).on_conflict_do_update(
            index_elements=['id'],
            set_=dict(rank=item.get('rank'), degree=item.get('degree'), url_id=item['urlId'])
        )
        await session.execute(stmt_emp)
        
        await session.execute(delete(DepartmentEmployee).where(DepartmentEmployee.employee_id == item['id']))
        api_depts = item.get('academicDepartment', [])
        links = set()
        
        for d_entry in api_depts:
            d_val = None
            if isinstance(d_entry, str):
                d_val = d_entry
            elif isinstance(d_entry, dict):
                d_val = d_entry.get('name') or d_entry.get('abbrev')
            
            if d_val:
                did = dept_map.get(d_val.strip().lower())
                if did: links.add(did)
        
        if links:
            vals = [{"department_id": did, "employee_id": item['id']} for did in links]
            await session.execute(pg_insert(DepartmentEmployee).values(vals).on_conflict_do_nothing())

    await session.commit()

async def sync_auditories(session: AsyncSession, client: BsuirApiClient):
    logger.info("Синхронизация аудиторий (Fixing Names)...")
    data = await client.get_auditories()
    
    res = await session.execute(select(Department.id))
    existing_dept_ids = set(res.scalars().all())

    for item in data:
        raw_name = item['name']
        build_obj = item.get('buildingNumber') or {}
        build_name = build_obj.get('name') if isinstance(build_obj, dict) else None
        
        if not build_name and item.get('buildingNumberId'):
             build_name = f"{item['buildingNumberId']} к."

        if build_name and build_name not in raw_name:
            final_name = f"{raw_name}-{build_name}"
        else:
            final_name = raw_name

        dept_info = item.get('department')
        dept_id = item.get('departmentId')
        
        if dept_info and isinstance(dept_info, dict):
            d_id = dept_info.get('idDepartment')
            if d_id and d_id not in existing_dept_ids:
                d_name = dept_info.get('name') or f"Dept {d_id}"
                d_abbr = dept_info.get('abbrev') or f"D-{d_id}"
                stmt_d = pg_insert(Department).values(
                    id=d_id, name=d_name, abbr=d_abbr, url_id=str(d_id)
                ).on_conflict_do_nothing()
                await session.execute(stmt_d)
                existing_dept_ids.add(d_id)
                dept_id = d_id
        
        if dept_id and dept_id not in existing_dept_ids:
            dept_id = None

        stmt = pg_insert(Auditory).values(
            id=item['id'],
            name=final_name,
            building_number=str(build_name)[:10] if build_name else None,
            note=item.get('note'),
            capacity=item.get('capacity'),
            auditory_type=item.get('auditoryType', {}).get('name'),
            department_id=dept_id
        ).on_conflict_do_update(
            index_elements=['id'],
            set_=dict(name=final_name, capacity=item.get('capacity'))
        )
        await session.execute(stmt)
    await session.commit()

async def _process_schedule_json(
    session: AsyncSession, entity_name: str, entity_type: str, data: dict, employee_id: int = None
):
    if entity_type == 'group':
        filter_cond = (ScheduleJsonStorage.group_name == entity_name)
    else:
        if not employee_id:
             logger.warning(f"SKIP JSON for {entity_name}: employee_id is None")
             return
        filter_cond = (ScheduleJsonStorage.employee_id == employee_id)

    await session.execute(
        update(ScheduleJsonStorage)
        .where(filter_cond, ScheduleJsonStorage.entity_type == entity_type, ScheduleJsonStorage.valid_to.is_(None))
        .values(valid_to=func.now())
    )

    new_json = ScheduleJsonStorage(
        group_name=entity_name if entity_type == 'group' else None,
        employee_id=employee_id if entity_type == 'employee' else None,
        entity_type=entity_type, 
        data=data,
        api_last_update_ts=datetime.now(), 
        valid_from=func.now()
    )
    session.add(new_json)
    await session.flush()
    
    events = []
    schedules = data.get('schedules', {}) or {}
    
    if entity_type == 'group':
        student_count_found = None
        if isinstance(schedules, dict):
            for lessons in schedules.values():
                for l in lessons:
                    groups_list = l.get('studentGroups') or []
                    for g in groups_list:
                        if isinstance(g, dict) and g.get('name') == entity_name:
                            cnt = g.get('numberOfStudents')
                            if cnt and cnt > 0:
                                student_count_found = cnt
                                break
                    if student_count_found: break
                if student_count_found: break
        
        if student_count_found:
            await session.execute(
                update(StudentGroup)
                .where(StudentGroup.name == entity_name, StudentGroup.valid_to.is_(None))
                .values(number_of_students=student_count_found)
            )

    if isinstance(schedules, dict):
        for day_name, lessons in schedules.items():
            day_num = DAYS_MAP.get(day_name)
            if not day_num or not lessons: continue
            
            for lesson in lessons:
                try:
                    s_time = datetime.strptime(lesson['startLessonTime'], '%H:%M').time()
                    e_time = datetime.strptime(lesson['endLessonTime'], '%H:%M').time()
                except: continue 

                aud_names = _extract_aud_names(lesson.get('auditories', []))
                weeks = _parse_weeks(lesson.get('weekNumber', []))
                
                subj = lesson.get('subject', 'Без названия') or 'Без названия'
                subj_full = lesson.get('subjectFullName') or subj
                
                emp_names = _extract_names_safe(lesson.get('employees', []), key='fio')
                grp_names = _extract_names_safe(lesson.get('studentGroups', []), key='name')

                search_parts = [str(subj), str(subj_full), str(entity_name), " ".join(aud_names)]
                if entity_type == 'group':
                    search_parts.extend(emp_names)
                else:
                    search_parts.extend(grp_names)
                
                events.append({
                    "entity_name": entity_name, "entity_type": entity_type,
                    "subject": subj, "subject_full": subj_full,
                    "auditories": aud_names, "day_of_week": day_num,
                    "start_time": s_time, "end_time": e_time, "week_numbers": weeks,
                    "exact_date": None,
                    "related_groups": lesson.get('studentGroups', []),
                    "related_employees": lesson.get('employees', []),
                    "subgroup": lesson.get('numSubgroup', 0),
                })

    exams = data.get('exams', []) or []
    for exam in exams:
        date_obj = _parse_date(exam.get('dateLesson'))
        if not date_obj: continue 
        try:
            s_time = datetime.strptime(exam['startLessonTime'], '%H:%M').time()
            e_time = datetime.strptime(exam['endLessonTime'], '%H:%M').time()
        except: 
            s_time = datetime.strptime("00:00", "%H:%M").time()
            e_time = datetime.strptime("00:00", "%H:%M").time()

        aud_names = _extract_aud_names(exam.get('auditories', []))
        subj = exam.get('subject', 'Экзамен') or 'Экзамен'
        subj_full = exam.get('subjectFullName') or subj
        
        events.append({
            "entity_name": entity_name, "entity_type": entity_type,
            "subject": subj, "subject_full": subj_full,
            "auditories": aud_names, "day_of_week": None,
            "start_time": s_time, "end_time": e_time, "week_numbers": [],
            "exact_date": date_obj,
            "related_groups": exam.get('studentGroups', []),
            "related_employees": exam.get('employees', []),
            "subgroup": exam.get('numSubgroup', 0),
        })

    await session.execute(delete(ScheduleEvent).where(
        ScheduleEvent.entity_name == entity_name, ScheduleEvent.entity_type == entity_type
    ))
    
    if events:
        await session.execute(insert(ScheduleEvent), events)
        
        sql_update_vector = """
        UPDATE schedule_events
        SET search_vector = to_tsvector('russian', 
            coalesce(subject, '') || ' ' || 
            coalesce(subject_full, '') || ' ' || 
            entity_name || ' ' || 
            array_to_string(auditories, ' ')
        )
        WHERE entity_name = :ename AND entity_type = :etype
        """
        await session.execute(text(sql_update_vector), {"ename": entity_name, "etype": entity_type})

async def sync_all_group_schedules(session: AsyncSession, client: BsuirApiClient):
    res = await session.execute(select(StudentGroup.name).where(StudentGroup.valid_to.is_(None)))
    group_names = res.scalars().all()
    logger.info(f"Обновление расписаний для {len(group_names)} групп...")
    
    for idx, name in enumerate(group_names):
        try:
            data = await client.get_group_schedule(name)
            if not data: continue

            async with session.begin_nested():
                await _process_schedule_json(session, name, 'group', data)
            
            await session.commit()
            if idx > 0 and idx % 50 == 0:
                logger.info(f"Прогресс групп: {idx} / {len(group_names)}")
        except Exception as e:
            logger.error(f"Ошибка группы {name}: {e}")
            await session.rollback()

async def sync_all_employee_schedules(session: AsyncSession, client: BsuirApiClient):
    res = await session.execute(select(Employee).where(Employee.url_id.is_not(None)))
    employees = res.scalars().all()
    logger.info(f"Обновление расписаний для {len(employees)} преподавателей...")
    
    for idx, emp in enumerate(employees):
        try:
            data = await client.get_employee_schedule(emp.url_id)
            if not data or (not data.get('schedules') and not data.get('exams')): continue

            async with session.begin_nested():
                await _process_schedule_json(session, emp.url_id, 'employee', data, employee_id=emp.id)
            
            await session.commit()
            if idx > 0 and idx % 50 == 0:
                logger.info(f"Прогресс сотрудников: {idx} / {len(employees)}")
                
        except Exception as e:
            logger.error(f"Ошибка преподавателя {emp.url_id}: {e}")
            await session.rollback()

async def rebuild_occupancy_index(session: AsyncSession):
    logger.info("Перестройка Occupancy Index...")
    await session.execute(text("TRUNCATE TABLE occupancy_index RESTART IDENTITY"))
    
    sql = """
    INSERT INTO occupancy_index (day_of_week, week_number, start_time, end_time, auditory_id, groups)
    SELECT 
        CASE se.day_of_week 
            WHEN 1 THEN 'Понедельник' WHEN 2 THEN 'Вторник' WHEN 3 THEN 'Среда' 
            WHEN 4 THEN 'Четверг' WHEN 5 THEN 'Пятница' WHEN 6 THEN 'Суббота' WHEN 7 THEN 'Воскресенье' 
        END,
        unnested_weeks.week_num,
        se.start_time,
        se.end_time,
        a.id,
        array_agg(DISTINCT se.entity_name)
    FROM schedule_events se
    CROSS JOIN LATERAL unnest(se.week_numbers) as unnested_weeks(week_num)
    CROSS JOIN LATERAL unnest(se.auditories) as unnested_auds(aud_name)
    JOIN auditories a ON a.name = unnested_auds.aud_name
    WHERE se.entity_type = 'group' AND se.day_of_week IS NOT NULL
    GROUP BY se.day_of_week, unnested_weeks.week_num, se.start_time, se.end_time, a.id;
    """
    try:
        await session.execute(text(sql))
        await session.commit()
        logger.success("Occupancy Index успешно перестроен.")
    except Exception as e:
        logger.error(f"Ошибка при построении индекса: {e}")
        await session.rollback()