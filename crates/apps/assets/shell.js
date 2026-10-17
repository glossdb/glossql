// The bar's pickers: each option's value is the path it opens, and
// choosing one is navigation. The page carries no handler of its own
// — the door's content security policy runs the shell's assets and
// nothing inline. Delegated on the document, because hx-boost swaps
// the body and a listener on the picker would go with it.
(function () {
  'use strict';

  document.addEventListener('change', (event) => {
    const picker = event.target.closest('select[data-nav]');
    if (picker) location.assign(picker.value);
  });
})();
