from sqlalchemy import Column, BigInteger, String, Integer, Time, Date, ForeignKey, JSON, TIMESTAMP, ARRAY, Text
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import TSVECTOR


Base = declarative_base()


class SystemState(Base):
    __tablename__ = "system_state"
    key = Column(String(50), primary_key=True)
    value = Column(String(255), nullable=False)
    updated_at = Column(TIMESTAMP, server_default=func.now(), onupdate=func.now())


class Faculty(Base):
    __tablename__ = "faculties"
    id = Column(BigInteger, primary_key=True)
    name = Column(String(500), nullable=False, unique=True)
    abbr = Column(String(50), nullable=False, unique=True)


class Department(Base):
    __tablename__ = "departments"
    id = Column(BigInteger, primary_key=True)
    name = Column(String(500), nullable=False, unique=True)
    abbr = Column(String(50), nullable=False, unique=True)
    url_id = Column(String(100), nullable=False, unique=True)


class Employee(Base):
    __tablename__ = "employees"
    id = Column(BigInteger, primary_key=True)
    first_name = Column(String(100), nullable=False)
    last_name = Column(String(100), nullable=False)
    middle_name = Column(String(100), nullable=True)
    degree = Column(String(100), nullable=True)
    rank = Column(String(100), nullable=True)
    photo_link = Column(String(500), nullable=True)
    calendar_id = Column(String(500), nullable=True)
    url_id = Column(String(100), nullable=False, unique=True)


class DepartmentEmployee(Base):
    __tablename__ = "departments_employees"
    department_id = Column(BigInteger, ForeignKey("departments.id"), primary_key=True)
    employee_id = Column(BigInteger, ForeignKey("employees.id"), primary_key=True)


class Speciality(Base):
    __tablename__ = "specialities"
    id = Column(BigInteger, primary_key=True)
    name = Column(String(500), nullable=False)
    abbr = Column(String(100), nullable=False)
    code = Column(String(45), nullable=False)
    education_form = Column(String(100), nullable=False)
    faculty_id = Column(BigInteger, ForeignKey("faculties.id"), nullable=False)


class StudentGroup(Base):
    __tablename__ = "student_groups"
    surrogate_id = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(BigInteger, nullable=False) # Not unique (SCD2)
    name = Column(String(50), nullable=False)
    course = Column(Integer, nullable=True)
    calendar_id = Column(String(500), nullable=True)
    education_degree = Column(Integer, nullable=False)
    number_of_students = Column(Integer, nullable=True)
    specialty_id = Column(BigInteger, ForeignKey("specialities.id"), nullable=False)
    valid_from = Column(TIMESTAMP, server_default=func.now())
    valid_to = Column(TIMESTAMP, nullable=True)


class Auditory(Base):
    __tablename__ = "auditories"
    id = Column(BigInteger, primary_key=True)
    name = Column(String(100), nullable=False)
    building_number = Column(String(10), nullable=True)
    note = Column(String(255), nullable=True)
    capacity = Column(Integer, nullable=True)
    auditory_type = Column(String(100), nullable=True)
    department_id = Column(BigInteger, ForeignKey("departments.id"), nullable=True)


class OccupancyIndex(Base):
    __tablename__ = "occupancy_index"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    day_of_week = Column(String(20), nullable=False)
    week_number = Column(Integer, nullable=False)
    start_time = Column(Time, nullable=False)
    end_time = Column(Time, nullable=False)
    auditory_id = Column(BigInteger, ForeignKey("auditories.id"), nullable=False)
    groups = Column(ARRAY(Text), nullable=False)


class ScheduleJsonStorage(Base):
    __tablename__ = "schedule_json_storage"
    id = Column(Integer, primary_key=True, autoincrement=True)
    group_name = Column(String(100), nullable=True)
    employee_id = Column(BigInteger, ForeignKey("employees.id"), nullable=True)
    entity_type = Column(String(50), nullable=False)
    data = Column(JSON, nullable=False)
    api_last_update_ts = Column(TIMESTAMP, nullable=True)
    valid_from = Column(TIMESTAMP, server_default=func.now())
    valid_to = Column(TIMESTAMP, nullable=True)


class ScheduleEvent(Base):
    __tablename__ = "schedule_events"
    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_name = Column(String(500), nullable=False)
    entity_type = Column(String(50), nullable=False)
    subject = Column(String(255), nullable=False)
    subject_full = Column(String(255), nullable=True)
    auditories = Column(ARRAY(Text), nullable=False)
    day_of_week = Column(Integer, nullable=True)
    start_time = Column(Time, nullable=False)
    end_time = Column(Time, nullable=False)
    week_numbers = Column(ARRAY(Integer), nullable=False)
    exact_date = Column(Date, nullable=True)
    related_groups = Column(JSON, nullable=True)
    related_employees = Column(JSON, nullable=True)
    subgroup = Column(Integer, default=0)
    search_vector = Column(TSVECTOR, nullable=True)