const runBtn = document.getElementById('runBtn');
const exportBtn = document.getElementById('exportBtn');
const sapFile = document.getElementById('sapFile');
const osFile = document.getElementById('osFile');
const hideZeros = document.getElementById('hideZeros');
const statusEl = document.getElementById('status');
const resultsPanel = document.getElementById('resultsPanel');
const overlay = document.getElementById('overlay');
let latestResults = null;
const expanded = new Set(['4100000']);
const NON_EXPANDABLE_LABELS = new Set([
  'Operating Expense (Ex-D And A)',
  'EBITDA',
  'EBIT',
  'Profit Or Loss Before Tax',
  'Net Profit Or Loss (PAT)',
  'Profit Or Loss Attributable To Owners Of The Company'
]);

function fmt(v){
  if(v === null || v === undefined || v === '') return '';
  const n = Number(v);
  if(!Number.isNaN(n)) return n.toLocaleString('en-GB', {minimumFractionDigits:0, maximumFractionDigits:2});
  return String(v);
}
function isZeroish(v){ return Math.abs(Number(v || 0)) < 0.0000001; }
function shouldHide(row){
  return hideZeros.checked && isZeroish(row.sap_bfc) && isZeroish(row.onestream);
}
function setStatus(msg, cls='ok'){
  statusEl.textContent = msg;
  statusEl.className = 'status ' + cls;
}
function showLoading(on){ overlay.classList.toggle('show', on); }

document.querySelectorAll('.tab-btn[data-tab]').forEach(btn => {
  btn.addEventListener('click', () => {
    document.querySelectorAll('.tab-btn[data-tab]').forEach(x => x.classList.toggle('active', x === btn));
    document.querySelectorAll('.tab-pane').forEach(p => p.classList.toggle('active', p.id === btn.dataset.tab));
  });
});
hideZeros.addEventListener('change', () => { if(latestResults) renderResults(latestResults); });

function makeFormData(){
  const fd = new FormData();
  if(!sapFile.files[0] || !osFile.files[0]) throw new Error('Please upload both files first.');
  fd.append('sap_file', sapFile.files[0]);
  fd.append('os_file', osFile.files[0]);
  return fd;
}

function renderTable(bodyId, rows, cols, rowFilter, rowClassFn){
  const body = document.getElementById(bodyId);
  body.innerHTML = '';
  rows.filter(r => !rowFilter || rowFilter(r)).forEach(row => {
    const tr = document.createElement('tr');
    if(rowClassFn) {
      const cls = rowClassFn(row);
      if(cls) tr.className = cls;
    }
    cols.forEach(col => {
      const td = document.createElement('td');
      if(col.num) td.classList.add('num');
      if(col.html){
        td.innerHTML = col.html(row);
      }else if(col.num){
        td.textContent = fmt(row[col.key]);
      }else{
        td.textContent = row[col.key] === null || row[col.key] === undefined ? '' : String(row[col.key]);
      }
      tr.appendChild(td);
    });
    body.appendChild(tr);
  });
}

function visibleDrillRows(rows){
  const visible = [];
  rows.forEach(row => {
    if(row.row_type === 'parent'){
      visible.push(row);
      return;
    }
    if(expanded.has(row.parent_code)) visible.push(row);
  });
  return visible;
}

function isNonExpandableRow(row){
  const label = String(row.name || '').trim();
  return NON_EXPANDABLE_LABELS.has(label);
}

function drillNameCell(row){
  if(row.row_type === 'parent'){
    if(isNonExpandableRow(row)) {
      return `<span>${row.name}</span>`;
    }
    const isOpen = expanded.has(row.code);
    return `<button class="drill-toggle" data-code="${row.code}"><span class="caret">${isOpen ? '▾' : '▸'}</span>${row.name}</button>`;
  }
  return `<span class="gl-code">${row.code}</span>`;
}

function renderDebug(){
  const dbg = latestResults.debug || {};
  const lines = [
    `SAP rows used: ${fmt(dbg.sap_rows)}`,
    `OneStream rows used: ${fmt(dbg.os_rows)}`,
    `OneStream non-zero rows: ${fmt(dbg.os_rows_with_amount)}`,
    `SAP GL codes grouped: ${fmt(dbg.sap_gl_mapped)}`,
    `OneStream GL codes grouped: ${fmt(dbg.os_gl_mapped)}`,
    `OneStream GL codes matched to mapping: ${fmt(dbg.os_gl_codes_matched_to_mapping)}`,
    `OneStream GL codes still unmapped: ${fmt(dbg.os_gl_codes_unmapped)}`,
    `Combined GL codes: ${fmt(dbg.all_gl_codes)}`,
    `SAP total: ${fmt(dbg.sap_total_all_rows)}`,
    `OneStream total: ${fmt(dbg.os_total_all_rows)}`,
    `Unmapped GL codes: ${fmt(dbg.unmapped_gl_codes)}`,
    '',
    'Top unmapped GL codes:'
  ];
  (dbg.unmapped_top_items || []).forEach((item, i) => {
    lines.push(`${i + 1}. ${item.gl_code || '(blank)'} | ${item.description || ''} | SAP ${fmt(item.sap_bfc)} | OS ${fmt(item.onestream)} | Diff ${fmt(item.difference)}`);
  });
  document.getElementById('debugBox').textContent = lines.join('\n');
}

function renderResults(data){
  latestResults = data;
  resultsPanel.style.display = 'block';
  document.getElementById('metaSapRows').textContent = fmt(data.debug.sap_rows);
  document.getElementById('metaOsRows').textContent = fmt(data.debug.os_rows);
  document.getElementById('metaGlCodes').textContent = fmt(data.debug.all_gl_codes);
  document.getElementById('metaUnmapped').textContent = fmt(data.debug.unmapped_gl_codes);

  renderTable('summaryBody', data.summary_rows, [
    {key:'name'},
    {key:'sap_bfc', num:true},
    {key:'onestream', num:true},
    {key:'difference', num:true}
  ], row => !shouldHide(row), row => row.highlight ? 'row-highlight' : '');

  renderTable('drillBody', visibleDrillRows(data.drilldown_rows), [
    {html: drillNameCell},
    {key:'description'},
    {key:'currency'},
    {key:'sap_bfc', num:true},
    {key:'onestream', num:true},
    {key:'difference', num:true}
  ], row => row.row_type === 'parent' || !shouldHide(row), row => {
    if(row.row_type === 'parent' && row.highlight) return 'row-highlight row-parent';
    if(row.row_type === 'parent') return 'row-parent';
    return 'row-child';
  });

  document.querySelectorAll('.drill-toggle').forEach(btn => {
    btn.addEventListener('click', () => {
      const code = btn.dataset.code;
      if(expanded.has(code)) expanded.delete(code); else expanded.add(code);
      renderResults(latestResults);
    });
  });

  renderDebug();
}

runBtn.addEventListener('click', async () => {
  try{
    setStatus('Running reconciliation...', 'ok');
    showLoading(true);
    const res = await fetch('/api/run-recon', { method:'POST', body: makeFormData() });
    const data = await res.json();
    if(!res.ok || data.error) throw new Error(data.error || 'Failed to run reconciliation.');
    renderResults(data);
    setStatus('Reconciliation completed.', 'ok');
  }catch(err){
    setStatus(err.message || String(err), 'bad');
  }finally{
    showLoading(false);
  }
});

if(exportBtn){
  exportBtn.addEventListener('click', async () => {
    try{
      setStatus('Preparing Excel...', 'ok');
      const res = await fetch('/api/export', { method:'POST', body: makeFormData() });
      if(!res.ok){
        let data = {};
        try{ data = await res.json(); }catch(_){ }
        throw new Error(data.error || 'Export failed.');
      }
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'reconciliation_output_v9.xlsx';
      document.body.appendChild(a);
      a.click();
      a.remove();
      URL.revokeObjectURL(url);
      setStatus('Excel downloaded.', 'ok');
    }catch(err){
      setStatus(err.message || String(err), 'bad');
    }
  });
}
