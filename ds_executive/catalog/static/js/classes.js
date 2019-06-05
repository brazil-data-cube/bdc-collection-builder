var Pager = class Pager {
    constructor(id, listId, items, itemsPerPage) {
        this.id = id;
        this.listId = listId;
        this.items = items;
        this.pagedItems = [];
        this.currentPage = 0;
        this.itemsPerPage = itemsPerPage || 5;
        this.numOfPages = Math.ceil(items.length / itemsPerPage, 1);
    }

    bindList() {
        var pgItems = this.pagedItems[this.currentPage];
        $('#' + this.listId).empty();
        for (var i = 0; i < pgItems.length; i++) {
            var option = $('<a class="list-group-item list-group-item-action" data-toggle="list" role="tab">');
            option.html(pgItems[i].drd);
            $('#' + this.listId).append(option);
        }
    }

    prevPage() {
        console.log(this.currentPage)
        if (this.currentPage > 0) {
            this.currentPage--;
        }
        this.bindList();
    }

    nextPage() {
        if (this.currentPage < this.pagedItems.length - 1) {
            this.currentPage++;
        }
        this.bindList();
    }

    numPage(num) {
        this.currentPage = num - 1;
        this.bindList();
    }

    pagerInit() {
        var pager = this

        for (var i = 0; i < this.items.length; i++) {
            if (i % this.itemsPerPage === 0) {
                this.pagedItems[Math.floor(i / this.itemsPerPage)] = [this.items[i]];
            } else {
                this.pagedItems[Math.floor(i / this.itemsPerPage)].push(this.items[i]);
            }
        }

        this.bindList();

        var pid = this.id + 'Prev'
        var nid = this.id + 'Next'

        var prev = $(`<li class="page-item">
                        <a class="page-link" href="#" id=${pid} aria-label="Previous">
                            <span aria-hidden="true">&laquo;</span>
                            <span class="sr-only">Previous</span>
                        </a>
                      </li>`);

        var next = $(`<li class="page-item">
                            <a class="page-link" href="#" id=${nid} aria-label="Next">
                                <span aria-hidden="true">&raquo;</span>
                                <span class="sr-only">Next</span>
                            </a>
                        </li>`);

        $('#' + this.id).append(prev);
        $('#' + pid).click(function () {
            pager.prevPage();
            return false;
        });

        for (var i = 1; i <= this.numOfPages; i++) {
            var option = $('<li class="page-item"><a class="page-link" id="' + i + '">' + i + '</a></li>');
            $('#' + this.id).append(option);
            $('#' + i).click(i, function (id) {
                pager.numPage(id.data);
                return false;
            });
        }

        $('#' + this.id).append(next);
        $('#' + nid).click(function () {
            pager.nextPage();
            return false;
        });
    }
};

var DatePicker = class DatePicker {
    constructor(id) {
        this.id = id;
        this.startPicker()
        this.endPicker()
    }

    startPicker() {
        $('input').filter('.datepicker-start').each(function () {
            $(this).datepicker($.extend({
                onSelect: function () {
                    var minDate = $(this).datepicker('getDate');
                    minDate.setDate(minDate.getDate());
                    $("#endPicker" + $(this).data().bind).datepicker("option", "minDate", minDate);
                }
            }, { dateFormat: "yy-mm-dd" }));
        });
    }

    endPicker() {
        $('input').filter('.datepicker-end').each(function () {
            $(this).datepicker($.extend({
                onSelect: function () {
                    var maxDate = $(this).datepicker('getDate');
                    maxDate.setDate(maxDate.getDate());
                    $("#startPicker" + $(this).data().bind).datepicker("option", "maxDate", maxDate);
                }
            }, { dateFormat: "yy-mm-dd" }));
        });
    }
};