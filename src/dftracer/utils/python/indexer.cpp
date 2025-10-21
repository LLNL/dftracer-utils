#include <dftracer/utils/python/indexer.h>
#include <dftracer/utils/python/indexer_checkpoint.h>
#include <structmember.h>

static void Indexer_dealloc(IndexerObject *self) {
    if (self->handle) {
        dft_indexer_destroy(self->handle);
    }
    Py_XDECREF(self->gz_path);
    Py_XDECREF(self->idx_path);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static PyObject *Indexer_new(PyTypeObject *type, PyObject *args,
                             PyObject *kwds) {
    IndexerObject *self;
    self = (IndexerObject *)type->tp_alloc(type, 0);
    if (self != NULL) {
        self->handle = NULL;
        self->gz_path = NULL;
        self->idx_path = NULL;
        self->checkpoint_size = 0;
    }
    return (PyObject *)self;
}

static int Indexer_init(IndexerObject *self, PyObject *args, PyObject *kwds) {
    static const char *kwlist[] = {"gz_path", "idx_path", "checkpoint_size",
                                   "force_rebuild", NULL};
    const char *gz_path;
    const char *idx_path = NULL;
    std::uint64_t checkpoint_size =
        dftracer::utils::constants::indexer::DEFAULT_CHECKPOINT_SIZE;
    int force_rebuild = 0;

    if (!PyArg_ParseTupleAndKeywords(args, kwds, "s|snp", (char **)kwlist,
                                     &gz_path, &idx_path, &checkpoint_size,
                                     &force_rebuild)) {
        return -1;
    }

    self->gz_path = PyUnicode_FromString(gz_path);
    if (!self->gz_path) {
        return -1;
    }

    if (idx_path) {
        self->idx_path = PyUnicode_FromString(idx_path);
    } else {
        PyObject *gz_path_obj = PyUnicode_FromString(gz_path);
        self->idx_path = PyUnicode_FromFormat("%U.idx", gz_path_obj);
        Py_DECREF(gz_path_obj);
    }

    if (!self->idx_path) {
        Py_DECREF(self->gz_path);
        return -1;
    }

    self->checkpoint_size = checkpoint_size;

    const char *idx_path_str = PyUnicode_AsUTF8(self->idx_path);
    if (!idx_path_str) {
        return -1;
    }

    self->handle = dft_indexer_create(gz_path, idx_path_str, checkpoint_size,
                                      force_rebuild);
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to create indexer");
        return -1;
    }

    return 0;
}

static PyObject *Indexer_build(IndexerObject *self,
                               PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Indexer not initialized");
        return NULL;
    }

    int result = dft_indexer_build(self->handle);
    if (result != 0) {
        PyErr_SetString(PyExc_RuntimeError, "Failed to build index");
        return NULL;
    }

    Py_RETURN_NONE;
}

static PyObject *Indexer_need_rebuild(IndexerObject *self,
                                      PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Indexer not initialized");
        return NULL;
    }

    int result = dft_indexer_need_rebuild(self->handle);
    return PyBool_FromLong(result);
}

static PyObject *Indexer_exists(IndexerObject *self,
                                PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Indexer not initialized");
        return NULL;
    }

    int result = dft_indexer_exists(self->handle);
    return PyBool_FromLong(result);
}

static PyObject *Indexer_get_max_bytes(IndexerObject *self,
                                       PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Indexer not initialized");
        return NULL;
    }

    uint64_t result = dft_indexer_get_max_bytes(self->handle);
    return PyLong_FromUnsignedLongLong(result);
}

static PyObject *Indexer_get_num_lines(IndexerObject *self,
                                       PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Indexer not initialized");
        return NULL;
    }

    uint64_t result = dft_indexer_get_num_lines(self->handle);
    return PyLong_FromUnsignedLongLong(result);
}

static PyObject *Indexer_find_checkpoint(IndexerObject *self, PyObject *args) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Indexer not initialized");
        return NULL;
    }

    std::size_t target_offset;
    if (!PyArg_ParseTuple(args, "n", &target_offset)) {
        return NULL;
    }

    dft_indexer_checkpoint_t checkpoint;
    int found =
        dft_indexer_find_checkpoint(self->handle, target_offset, &checkpoint);

    if (!found) {
        Py_RETURN_NONE;
    }

    // Create IndexerCheckpoint object
    IndexerCheckpointObject *cp_obj =
        (IndexerCheckpointObject *)IndexerCheckpoint_new(&IndexerCheckpointType,
                                                         NULL, NULL);
    if (!cp_obj) {
        return NULL;
    }

    cp_obj->checkpoint = checkpoint;
    return (PyObject *)cp_obj;
}

static PyObject *Indexer_get_checkpoints(IndexerObject *self,
                                         PyObject *Py_UNUSED(ignored)) {
    if (!self->handle) {
        PyErr_SetString(PyExc_RuntimeError, "Indexer not initialized");
        return NULL;
    }

    dft_indexer_checkpoint_t *checkpoints = NULL;
    std::size_t count = 0;

    int result =
        dft_indexer_get_checkpoints(self->handle, &checkpoints, &count);
    if (result != 0 || !checkpoints) {
        dft_indexer_free_checkpoints(checkpoints, count);
        PyObject *list = PyList_New(0);
        return list;
    }

    PyObject *list = PyList_New(count);
    if (!list) {
        dft_indexer_free_checkpoints(checkpoints, count);
        return NULL;
    }

    for (std::size_t i = 0; i < count; i++) {
        IndexerCheckpointObject *cp_obj =
            (IndexerCheckpointObject *)IndexerCheckpoint_new(
                &IndexerCheckpointType, NULL, NULL);
        if (!cp_obj) {
            Py_DECREF(list);
            dft_indexer_free_checkpoints(checkpoints, count);
            return NULL;
        }
        cp_obj->checkpoint = checkpoints[i];
        PyList_SetItem(list, i, (PyObject *)cp_obj);
    }

    dft_indexer_free_checkpoints(checkpoints, count);
    return list;
}

static PyObject *Indexer_gz_path(IndexerObject *self, void *closure) {
    Py_INCREF(self->gz_path);
    return self->gz_path;
}

static PyObject *Indexer_idx_path(IndexerObject *self, void *closure) {
    Py_INCREF(self->idx_path);
    return self->idx_path;
}

static PyObject *Indexer_checkpoint_size(IndexerObject *self, void *closure) {
    return PyLong_FromUnsignedLongLong(self->checkpoint_size);
}

static PyObject *Indexer_enter(IndexerObject *self,
                               PyObject *Py_UNUSED(ignored)) {
    Py_INCREF(self);
    return (PyObject *)self;
}

static PyObject *Indexer_exit(IndexerObject *self, PyObject *args) {
    Py_RETURN_NONE;
}

static PyMethodDef Indexer_methods[] = {
    {"build", (PyCFunction)Indexer_build, METH_NOARGS,
     "Build or rebuild the index"},
    {"need_rebuild", (PyCFunction)Indexer_need_rebuild, METH_NOARGS,
     "Check if a rebuild is needed"},
    {"exists", (PyCFunction)Indexer_exists, METH_NOARGS,
     "Check if the index file exists"},
    {"get_max_bytes", (PyCFunction)Indexer_get_max_bytes, METH_NOARGS,
     "Get the maximum uncompressed bytes in the indexed file"},
    {"get_num_lines", (PyCFunction)Indexer_get_num_lines, METH_NOARGS,
     "Get the total number of lines in the indexed file"},
    {"find_checkpoint", (PyCFunction)Indexer_find_checkpoint, METH_VARARGS,
     "Find the best checkpoint for a given uncompressed offset"},
    {"get_checkpoints", (PyCFunction)Indexer_get_checkpoints, METH_NOARGS,
     "Get all checkpoints for this file as a list"},

    {"__enter__", (PyCFunction)Indexer_enter, METH_NOARGS,
     "Enter the runtime context for the with statement"},
    {"__exit__", (PyCFunction)Indexer_exit, METH_VARARGS,
     "Exit the runtime context for the with statement"},
    {NULL} /* Sentinel */
};

static PyGetSetDef Indexer_getsetters[] = {
    {"gz_path", (getter)Indexer_gz_path, NULL, "Path to the gzip file", NULL},
    {"idx_path", (getter)Indexer_idx_path, NULL, "Path to the index file",
     NULL},
    {"checkpoint_size", (getter)Indexer_checkpoint_size, NULL,
     "Checkpoint size in bytes", NULL},
    {NULL} /* Sentinel */
};

PyTypeObject IndexerType = {
    PyVarObject_HEAD_INIT(NULL, 0) "indexer.Indexer", /* tp_name */
    sizeof(IndexerObject),                            /* tp_basicsize */
    0,                                                /* tp_itemsize */
    (destructor)Indexer_dealloc,                      /* tp_dealloc */
    0,                                                /* tp_vectorcall_offset */
    0,                                                /* tp_getattr */
    0,                                                /* tp_setattr */
    0,                                                /* tp_as_async */
    0,                                                /* tp_repr */
    0,                                                /* tp_as_number */
    0,                                                /* tp_as_sequence */
    0,                                                /* tp_as_mapping */
    0,                                                /* tp_hash */
    0,                                                /* tp_call */
    0,                                                /* tp_str */
    0,                                                /* tp_getattro */
    0,                                                /* tp_setattro */
    0,                                                /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,         /* tp_flags */
    "DFTracerIndexer objects",                        /* tp_doc */
    0,                                                /* tp_traverse */
    0,                                                /* tp_clear */
    0,                                                /* tp_richcompare */
    0,                                                /* tp_weaklistoffset */
    0,                                                /* tp_iter */
    0,                                                /* tp_iternext */
    Indexer_methods,                                  /* tp_methods */
    0,                                                /* tp_members */
    Indexer_getsetters,                               /* tp_getset */
    0,                                                /* tp_base */
    0,                                                /* tp_dict */
    0,                                                /* tp_descr_get */
    0,                                                /* tp_descr_set */
    0,                                                /* tp_dictoffset */
    (initproc)Indexer_init,                           /* tp_init */
    0,                                                /* tp_alloc */
    Indexer_new,                                      /* tp_new */
};

int init_indexer(PyObject *m) {
    if (PyType_Ready(&IndexerType) < 0) return -1;

    Py_INCREF(&IndexerType);
    if (PyModule_AddObject(m, "Indexer", (PyObject *)&IndexerType) < 0) {
        Py_DECREF(&IndexerType);
        Py_DECREF(m);
        return -1;
    }

    return 0;
}
