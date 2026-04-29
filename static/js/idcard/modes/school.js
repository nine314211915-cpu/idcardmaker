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
    groups: [],
    directory: [],
    newGroupOption: '__new_group__',
    newSchoolOption: '__new_school__',

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
      this.setSchoolName(record.school_name || '', true);
      document.getElementById('schoolAddress').value = this.resolveSchoolAddress(record);
      document.getElementById('academicBatch').value = record.academic_batch || '';
    },

    clearContext() {
      this.commonFieldIds.forEach((id) => {
        const el = document.getElementById(id);
        if (el) el.value = '';
      });
      this.setSchoolName('', false);
    },

    setGroups(groups) {
      this.groups = Array.isArray(groups) ? groups : [];
      this.renderGroupSelect(document.getElementById('otherInstituteName').value);
    },

    renderGroupSelect(selectedGroup = '') {
      const select = document.getElementById('schoolGroupSelect');
      const input = document.getElementById('otherInstituteName');
      if (!select || !input) return;
      const normalizedSelected = String(selectedGroup || '').trim();
      const options = ['<option value="">Select institute / school group</option>'];
      this.groups.forEach((group, index) => {
        const selected = group && group.toLowerCase() === normalizedSelected.toLowerCase() ? ' selected' : '';
        options.push(`<option value="${index}"${selected}>${escapeHtml(group)}</option>`);
      });
      options.push(`<option value="${this.newGroupOption}">Add New Group</option>`);
      select.innerHTML = options.join('');
      const matched = this.groups.some((group) => String(group || '').toLowerCase() === normalizedSelected.toLowerCase());
      input.style.display = selectedGroup && !matched ? '' : 'none';
      select.style.display = '';
      if (!selectedGroup && !this.groups.length) {
        select.value = this.newGroupOption;
        input.style.display = '';
      }
    },

    applySelectedGroup() {
      const select = document.getElementById('schoolGroupSelect');
      const input = document.getElementById('otherInstituteName');
      if (!select || !input) return;
      if (select.value === this.newGroupOption) {
        input.value = '';
        input.style.display = '';
        input.focus();
        return;
      }
      const selected = this.groups[Number(select.value)];
      if (!selected) {
        input.value = '';
        input.style.display = 'none';
        return;
      }
      input.value = selected;
      input.style.display = 'none';
    },

    setDirectory(schools) {
      this.directory = Array.isArray(schools) ? schools : [];
      this.renderSchoolSelect(document.getElementById('schoolName').value);
    },

    renderSchoolSelect(selectedName = '') {
      const select = document.getElementById('schoolNameSelect');
      const input = document.getElementById('schoolName');
      if (!select || !input) return;
      const normalizedSelected = String(selectedName || '').trim();
      const options = ['<option value="">Select school</option>'];
      this.directory.forEach((school, index) => {
        const name = school.school_name || '';
        const selected = name && name.toLowerCase() === normalizedSelected.toLowerCase() ? ' selected' : '';
        options.push(`<option value="${index}"${selected}>${escapeHtml(name)}</option>`);
      });
      options.push(`<option value="${this.newSchoolOption}">Add New School</option>`);
      select.innerHTML = options.join('');
      const matched = this.directory.some((school) => String(school.school_name || '').toLowerCase() === normalizedSelected.toLowerCase());
      input.style.display = selectedName && !matched ? '' : 'none';
      if (!selectedName && !this.directory.length) {
        select.value = this.newSchoolOption;
        input.style.display = '';
      }
    },

    setSchoolName(name, preferExisting = false) {
      const input = document.getElementById('schoolName');
      if (!input) return;
      input.value = name || '';
      this.renderSchoolSelect(input.value);
      if (preferExisting && input.value) {
        const matchIndex = this.directory.findIndex((school) => String(school.school_name || '').toLowerCase() === input.value.toLowerCase());
        if (matchIndex >= 0) {
          document.getElementById('schoolNameSelect').value = String(matchIndex);
          input.style.display = 'none';
        }
      }
    },

    applySelectedSchool() {
      const select = document.getElementById('schoolNameSelect');
      const input = document.getElementById('schoolName');
      if (!select || !input) return;
      if (select.value === this.newSchoolOption) {
        input.style.display = '';
        input.focus();
        return;
      }
      const selected = this.directory[Number(select.value)];
      if (!selected) {
        input.value = '';
        input.style.display = 'none';
        return;
      }
      input.value = selected.school_name || '';
      document.getElementById('schoolAddress').value = selected.school_address || '';
      document.getElementById('academicBatch').value = selected.academic_batch || '';
      input.style.display = 'none';
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
