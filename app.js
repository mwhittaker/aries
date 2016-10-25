
window.onload = function(){

  // Mapping of DOM table id to data keys corresponding to table columns
  var column_keys = {
    'log': ['lsn', 'txn_id', 'type', 'page_id', 'prev_lsn'],
    'dirty-page-table': ['page_id', 'rec_lsn'],
    'transaction-table': ['txn_id', 'txn_status', 'last_lsn'],
    'buffer-pool': ['page_id', 'page_lsn', 'value'],
    'disk': ['page_id', 'page_lsn', 'value']
  };

  // Helper function to convert an object map to a list
  function mapToList(map, id_name) {
    var list = [];
    for (var key of Object.keys(map)) {
      var obj = {};
      obj[id_name] = key;
      for (var field of Object.keys(map[key])) {
        obj[field] = map[key][field];
      }
      list.push(obj);
    }
    return list;
  }

  // Given a DOM table id and data list/object, join the new data to the
  // DOM table using D3 joins. If prev_data is not undefined, then any
  // changes between the previous and new data should be highlighted.
  function updateTable(table_id, data, prev_data) {
    var columns = column_keys[table_id];
    var key = columns[0];
    if (!Array.isArray(data)) {
      data = mapToList(data, key);
      if (prev_data != null) {
        prev_data = mapToList(prev_data, key);
      }
    }

    var updated_rows = d3.select('table#' + table_id)
                         .select('tbody')
                         .selectAll('tr')
                         .data(data, function(d) {
                           return d ? d[key] : this.id;
                         })
                         .attr('class', function(d, i) {
                           if (prev_data != null) {
                            for (var objs of prev_data) {
                              if (objs[key] == this.id) {
                                return '';
                              }
                            }
                            return 'new-row';
                           }
                           return '';
                         });

    updated_rows.exit().remove();

    var cells = updated_rows.selectAll('td')
                            .data(function(row) {
                              return columns.map(function(column) {
                                return row[column];
                              });
                            })
                            .attr('class', function(d, i) {
                              if (prev_data != null) {
                                for (var objs of prev_data) {
                                  if (objs[key] == this.parentNode.id) {
                                    if (objs[columns[i]] !== d) {
                                      d3.select(this.parentNode)
                                        .attr('class', 'updated-row');
                                      return 'updated-cell';
                                    }
                                    return '';
                                  }
                                }
                              }
                              return '';
                            })
                            .text(function(d) { return d; });

    var new_rows = updated_rows.enter()
                               .append('tr')
                               .attr('id', function(d) {
                                 return d[key]
                               })
                               .attr('class', function(d, i) {
                                 if (prev_data != null) {
                                   for (var objs of prev_data) {
                                     if (objs[key] == this.id) {
                                       return '';
                                     }
                                   }
                                   return 'new-row';
                                 }
                                 return '';
                               });

    new_rows.selectAll('td')
            .data(function(row) {
              return columns.map(function(column) {
                return row[column];
              });
            })
            .enter()
            .append('td')
            .attr('class', function(d, i) {
              if (prev_data != null) {
                for (var objs of prev_data) {
                  if (objs[key] == this.parentNode.id) {
                    if (objs[columns[i]] !== d) {
                      d3.select(this.parentNode)
                        .attr('class', 'updated-row');
                      return 'updated-cell';
                    }
                    return '';
                  }
                }
              }
              return '';
            })
            .text(function(d) { return d; });
  }

  // Given a DOM span id and datum value, assign the new datum value to the
  // DOM span using D3 data bindings
  function updateValue(span_id, datum) {
    d3.select('span#' + span_id)
      .datum(datum)
      .text(function(d) {
        if (d != -1) {
          return d;
        }
      });
  }

  // Given the number of operations processed and position of the log record
  // being analyzed, renders a pointer on the commands list and log to
  // indicate the current position on each respectively.
  function updatePointers(num_ops_processed, log_position) {
    if (num_ops_processed != -1) {
      d3.select('#commands')
        .selectAll("li")
        .attr('class','');
      d3.select('#commands')
        .selectAll("li")
        .filter(function(d, i) { return i == num_ops_processed; })
        .attr('class','current-op');
      document.getElementById('commands').scrollTop = num_ops_processed * 30 - 90;
    }
    if (log_position != -1) {
      d3.select('#log')
        .selectAll("tr")
        .filter(function(d, i) { return i == log_position; })
        .attr('class','current-row');
    }
  }

  // Given a list of explanations, renders them in the explanation box
  function updateExplanation(explanation) {
    var update = d3.select('#explanation')
                   .selectAll('li')
                   .data(explanation);
    update.text(function(d) { return d; });
    update.enter()
          .append('li')
          .text(function(d) { return d; });
    update.exit().remove();
  }

  // Given a new phase, update the style of the UI
  function updateStyle(phase) {
    d3.select('span#phase')
      .attr('class', phase);
    d3.select('body')
      .attr('class', phase);
    d3.selectAll('.panel')
      .attr('class', 'panel ' + phase);
  }

  // Given a new state index, enable/disable the left and right arrows
  function updateArrows(stateIdx) {
    if (stateIdx == 0) {
      d3.select('#left-arrow')
        .attr('disabled', true);
    } else if (stateIdx == states.length - 1) {
      d3.select('#right-arrow')
        .attr('disabled', true);
    } else {
      d3.select('#left-arrow')
        .attr('disabled', null);
      d3.select('#right-arrow')
        .attr('disabled', null);
    }
  }

  // Given a new state, update the UI to reflect the state
  function onStateChanged(stateIdx) {
    var state = states[stateIdx];
    var prevState = {};
    if (stateIdx > 0) {
      prevState = states[stateIdx - 1];
    }
    updateArrows(stateIdx);
    updateTable('log', state.log, prevState.log);
    updateTable('dirty-page-table', state.dirty_page_table, prevState.dirty_page_table);
    updateTable('transaction-table', state.txn_table, prevState.txn_table);
    updateTable('buffer-pool', state.buffer_pool, prevState.buffer_pool);
    updateTable('disk', state.disk, prevState.disk);
    updateValue('lsn-flushed', state.num_flushed - 1);
    updateValue('phase', state.phase);
    updatePointers(state.num_ops_processed, state.log_position);
    updateExplanation(state.explanation);
    updateStyle(state.phase);
  }

  // Update the current UI mode
  function updateMode(newMode) {
    mode = newMode; // sets global 'mode' variable

    // Toggle the correct mode button
    d3.select('div#modes')
      .selectAll('button')
      .attr('class', null);
    d3.select('div#modes')
      .selectAll('button#' + mode)
      .attr('class', 'hidden');

    // Toggle the correct sub controls
    d3.select('div#sub-controls')
      .selectAll('div')
      .attr('class', 'hidden');
    d3.select('div#sub-controls')
      .select('div#sub-controls-' + mode)
      .attr('class', null);

    // Enable/disable editing the command list
    if (mode === 'run') {
      command_list.option('disabled', true);
      d3.select('ul#commands').attr('class', null);
      resetAries();
    } else {
      command_list.option('disabled', false);
      d3.select('ul#commands')
        .attr('class', 'editable')
        .selectAll("li")
        .attr('class','');
    }
  }

  // Append a new command to the current command list
  function appendCommand(command) {
    var el = document.createElement('li');
    el.innerHTML = '<span>' + command + '</span>' +
                   '<i class="fa fa-times remove"></i>' +
                   '<i class="fa fa-pencil edit"></i>';
    el.dataset.id = command;
    command_list.el.appendChild(el);
    command_list.save();
  }

  // Parse input commands and reset ARIES state
  function resetAries() {
    var input = command_list.toArray().join(',');
    var ops_result = aries.Op.parse_ops(input);
    if (!ops_result.status) {
      console.assert(false, Parsimmon.formatError(input, ops_result));
    }
    var ops = ops_result.value;

    // Start the ARIES simulator.
    states = aries.simulate(ops);
    currStateIdx = 0;
    onStateChanged(currStateIdx);
  }

  // Renders previous state
  function renderPrevState() {
    if (currStateIdx > 0) {
      onStateChanged(--currStateIdx);
    }
  }

  // Renders next state
  function renderNextState() {
    if (currStateIdx < states.length - 1) {
      onStateChanged(++currStateIdx);
    }
  }


  /**
   * Event listeners
   */

  // Set UI to run mode
  var run_button = document.getElementById('run');
  run_button.addEventListener('click', function(event) {
    updateMode("run");
  }, false);

  // Set UI to edit mode
  var edit_button = document.getElementById('edit');
  edit_button.addEventListener('click', function(event) {
    updateMode("edit");
  }, false);

  // Transition between states when left or right arrow keys are pressed
  document.addEventListener('keydown', function(event) {
    if (mode === "run") {
      if (event.keyCode === 37) { // left arrow
        renderPrevState();
      } else if (event.keyCode === 39) { // right arrow
        renderNextState();
      }
    }
  }, false);

  // Transition to previous state when left arrow button is clicked
  var left_arrow = document.getElementById('left-arrow');
  left_arrow.addEventListener('click', function(event) {
    if (mode === "run") {
      renderPrevState();
    }
  }, false);

  // Transition to next state when right arrow button is clicked
  var right_arrow = document.getElementById('right-arrow');
  right_arrow.addEventListener('click', function(event) {
    if (mode === "run") {
      renderNextState();
    }
  }, false);

  // Open a new command dialog when new command button is clicked
  var new_command = document.getElementById('new-command');
  new_command.addEventListener('click', function(event) {
    vex.dialog.open({
      message: 'New command:',
      input: '<input name="command" type="text" required>',
      callback: function(data) {
        if (data) {
          appendCommand(data.command);
        }
      }
    });
  }, false);

  // Sortable element containing list of commands
  var commands_el = document.getElementById('commands');
  var command_list = Sortable.create(commands_el, {
    filter: '.edit, .remove',
    onFilter: function (evt) {
      if (Sortable.utils.is(evt.target, ".remove")) {
        evt.item.parentNode.removeChild(evt.item);
        command_list.save();
      } else if (Sortable.utils.is(evt.target, ".edit")) {
        vex.dialog.open({
          message: 'Edit command:',
          input: '<input name="command" type="text" value="' +
                 evt.item.dataset.id + '" required>',
          callback: function(data) {
            if (data) {
              evt.item.firstChild.innerHTML = data.command;
              evt.item.dataset.id = data.command;
              command_list.save();
            }
          }
        });
      }
    }
  });

  // About content modal
  var about = document.getElementById('about');
  about.addEventListener('click', function(event) {
    event.preventDefault();
    var modalContent = d3.select('#about-content').html();
    vex.dialog.alert({ unsafeMessage: modalContent });
  }, false);


  /**
   * Initializer
   */

  // UI global variables
  var states,       // list of ARIES states
      currStateIdx, // index of the current state
      mode;         // UI mode, either "run" or "edit"

  // Initializes the UI
  function init() {
    var starter_commands = [
      "W_1(A, one)",
      "W_2(B, two)",
      "Flush(B)",
      "W_3(C, three)",
      "Flush(C)",
      "Checkpoint()",
      "W_2(D, four)",
      "W_1(A, five)",
      "Commit_1()",
      "W_3(C, six)",
      "W_2(D, seven)",
      "Flush(D)",
      "W_2(B, eight)",
      "W_3(A, nine)"
    ];

    for (var command of starter_commands) {
      appendCommand(command);
    }

    // set UI to run mode
    updateMode("run");
  }

  init();

}
