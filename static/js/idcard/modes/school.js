(function () {
  // Keep school-specific rules out of the shared institute page.
  // Future institute types should follow this mode-handler pattern.
  const schoolMode = {
    instituteOption: 'School / Educational Institute',
    classes: ['Nursery', 'LKG', 'UKG', 'Class 1', 'Class 2', 'Class 3', 'Class 4', 'Class 5', 'Class 6', 'Class 7', 'Class 8', 'Class 9', 'Class 10', 'Class 11', 'Class 12'],
    teacherDesignations: ['Principal', 'Vice Principal', 'Headmaster', 'Headmistress', 'PGT', 'TGT', 'PRT', 'Subject Teacher', 'Sports Teacher', 'Computer Teacher', 'Librarian'],
    commonFieldIds: ['schoolName', 'schoolAddress', 'academicBatch'],
    studentFieldIds: ['className', 'section', 'rollNo'],
    teacherFieldIds: ['subject', 'joiningDate'],

    isSelected(value) {
      return value === this.instituteOption;
    },

    resolveSchoolAddress(record) {
      return record.school_address || record.place || '';
    },

    buildContextPayload() {
      return {
        school_name: document.getElementById('schoolName').value,
        school_address: document.getElementById('schoolAddress').value,
        place: document.getElementById('schoolAddress').value,
        academic_batch: document.getElementById('academicBatch').value,
      };
    },

    fillContext(record) {
      document.getElementById('schoolName').value = record.school_name || '';
      document.getElementById('schoolAddress').value = this.resolveSchoolAddress(record);
      document.getElementById('academicBatch').value = record.academic_batch || '';
    },

    clearContext() {
      this.commonFieldIds.forEach((id) => {
        const el = document.getElementById(id);
        if (el) el.value = '';
      });
    },

    clearStudent() {
      this.studentFieldIds.forEach((id) => {
        const el = document.getElementById(id);
        if (el) el.value = '';
      });
    },

    clearTeacher() {
      this.teacherFieldIds.forEach((id) => {
        const el = document.getElementById(id);
        if (el) el.value = '';
      });
    },

    studentPayload() {
      return {
        class_name: document.getElementById('className').value,
        section: document.getElementById('section').value,
        roll_no: document.getElementById('rollNo').value,
      };
    },

    teacherPayload() {
      return {
        subject: document.getElementById('subject').value,
        joining_date: document.getElementById('joiningDate').value,
      };
    },

    fillStudent(record) {
      document.getElementById('className').value = record.class_name || record.course || '';
      document.getElementById('section').value = record.section || '';
      document.getElementById('rollNo').value = record.roll_no || '';
    },

    fillTeacher(record) {
      document.getElementById('subject').value = record.subject || '';
      document.getElementById('joiningDate').value = record.joining_date ? formatDate(record.joining_date) : '';
    },
  };

  window.SchoolMode = schoolMode;
})();
