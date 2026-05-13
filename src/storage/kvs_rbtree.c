#include "kvs_rbtree.h"
#include "common.h"


#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

rbtree_node *rbtree_mini(rbtree *T, rbtree_node *x) {
	assert(x != T->nil);
	assert(x != NULL);
	while (x->left != T->nil) {
		x = x->left;
	}
	return x;
}

rbtree_node *rbtree_maxi(rbtree *T, rbtree_node *x) {
	while (x->right != T->nil) {
		x = x->right;
	}
	return x;
}

rbtree_node *rbtree_successor(rbtree *T, rbtree_node *x) {
	rbtree_node *y = x->parent;

	if (x->right != T->nil) {
		return rbtree_mini(T, x->right);
	}

	while ((y != T->nil) && (x == y->right)) {
		x = y;
		y = y->parent;
	}
	return y;
}


void rbtree_left_rotate(rbtree *T, rbtree_node *x) {

	rbtree_node *y = x->right;  // x  --> y  ,  y --> x,   right --> left,  left --> right

	x->right = y->left; //1 1
	if (y->left != T->nil) { //1 2
		y->left->parent = x;
	}

	y->parent = x->parent; //1 3
	if (x->parent == T->nil) { //1 4
		T->root = y;
	} else if (x == x->parent->left) {
		x->parent->left = y;
	} else {
		x->parent->right = y;
	}

	y->left = x; //1 5
	x->parent = y; //1 6
}


void rbtree_right_rotate(rbtree *T, rbtree_node *y) {

	rbtree_node *x = y->left;

	y->left = x->right;
	if (x->right != T->nil) {
		x->right->parent = y;
	}

	x->parent = y->parent;
	if (y->parent == T->nil) {
		T->root = x;
	} else if (y == y->parent->right) {
		y->parent->right = x;
	} else {
		y->parent->left = x;
	}

	x->right = y;
	y->parent = x;
}

void rbtree_insert_fixup(rbtree *T, rbtree_node *z) {

	while (z->parent->color == RED) { //z ---> RED
		if (z->parent == z->parent->parent->left) {
			rbtree_node *y = z->parent->parent->right;
			if (y->color == RED) {
				z->parent->color = BLACK;
				y->color = BLACK;
				z->parent->parent->color = RED;

				z = z->parent->parent; //z --> RED
			} else {

				if (z == z->parent->right) {
					z = z->parent;
					rbtree_left_rotate(T, z);
				}

				z->parent->color = BLACK;
				z->parent->parent->color = RED;
				rbtree_right_rotate(T, z->parent->parent);
			}
		}else {
			rbtree_node *y = z->parent->parent->left;
			if (y->color == RED) {
				z->parent->color = BLACK;
				y->color = BLACK;
				z->parent->parent->color = RED;

				z = z->parent->parent; //z --> RED
			} else {
				if (z == z->parent->left) {
					z = z->parent;
					rbtree_right_rotate(T, z);
				}

				z->parent->color = BLACK;
				z->parent->parent->color = RED;
				rbtree_left_rotate(T, z->parent->parent);
			}
		}
		
	}

	T->root->color = BLACK;
}


void rbtree_insert(rbtree *T, rbtree_node *z) {

	rbtree_node *y = T->nil;
	rbtree_node *x = T->root;

	while (x != T->nil) {
		y = x;
#if ENABLE_KEY_CHAR

		if (z->len_key == x->len_key && memcmp(z->key, x->key, z->len_key) < 0){ //strcmp(z->key, x->key) < 0) {
			x = x->left;
		} else if (z->len_key == x->len_key && memcmp(z->key, x->key, z->len_key) > 0){ //strcmp(z->key, x->key) > 0) {
			x = x->right;
		} else {
			return ;
		}

#else
		if (z->key < x->key) {
			x = x->left;
		} else if (z->key > x->key) {
			x = x->right;
		} else { //Exist
			return ;
		}
#endif
	}

	z->parent = y;
	if (y == T->nil) {
		T->root = z;
#if ENABLE_KEY_CHAR
	} else if (z->len_key == y->len_key && memcmp(z->key, y->key, z->len_key) < 0) {
#else
	} else if (z->key < y->key) {
#endif
		y->left = z;
	} else {
		y->right = z;
	}

	z->left = T->nil;
	z->right = T->nil;
	z->color = RED;

	rbtree_insert_fixup(T, z);
}

void rbtree_delete_fixup(rbtree *T, rbtree_node *x) {

	while ((x != T->root) && (x->color == BLACK)) {
		if (x == x->parent->left) {

			rbtree_node *w= x->parent->right;
			if (w->color == RED) {
				w->color = BLACK;
				x->parent->color = RED;

				rbtree_left_rotate(T, x->parent);
				w = x->parent->right;
			}

			if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
				w->color = RED;
				x = x->parent;
			} else {

				if (w->right->color == BLACK) {
					w->left->color = BLACK;
					w->color = RED;
					rbtree_right_rotate(T, w);
					w = x->parent->right;
				}

				w->color = x->parent->color;
				x->parent->color = BLACK;
				w->right->color = BLACK;
				rbtree_left_rotate(T, x->parent);

				x = T->root;
			}

		} else {

			rbtree_node *w = x->parent->left;
			if (w->color == RED) {
				w->color = BLACK;
				x->parent->color = RED;
				rbtree_right_rotate(T, x->parent);
				w = x->parent->left;
			}

			if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
				w->color = RED;
				x = x->parent;
			} else {

				if (w->left->color == BLACK) {
					w->right->color = BLACK;
					w->color = RED;
					rbtree_left_rotate(T, w);
					w = x->parent->left;
				}

				w->color = x->parent->color;
				x->parent->color = BLACK;
				w->left->color = BLACK;
				rbtree_right_rotate(T, x->parent);

				x = T->root;
			}

		}
	}

	x->color = BLACK;
}

rbtree_node *rbtree_delete(rbtree *T, rbtree_node *z) {

	rbtree_node *y = T->nil;
	rbtree_node *x = T->nil;

	if ((z->left == T->nil) || (z->right == T->nil)) {
		y = z;
	} else {
		y = rbtree_successor(T, z);
	}

	if (y->left != T->nil) {
		x = y->left;
	} else if (y->right != T->nil) {
		x = y->right;
	}

	x->parent = y->parent;
	if (y->parent == T->nil) {
		T->root = x;
	} else if (y == y->parent->left) {
		y->parent->left = x;
	} else {
		y->parent->right = x;
	}

	if (y != z) {
#if ENABLE_KEY_CHAR

		void *tmp = z->key;
		z->key = y->key;
		y->key = tmp;

		tmp = z->value;
		z->value= y->value;
		y->value = tmp;

#else
		z->key = y->key;
		z->value = y->value;
#endif
	}

	if (y->color == BLACK) {
		rbtree_delete_fixup(T, x);
	}

	return y;
}

rbtree_node *rbtree_search(rbtree *T, KEY_TYPE key, int len_key) {

	rbtree_node *node = T->root;
	while (node != T->nil) {
#if ENABLE_KEY_CHAR

		if (len_key == node->len_key && memcmp(key, node->key, len_key) < 0) {
			node = node->left;
		} else if (len_key == node->len_key && memcmp(key, node->key, len_key) > 0) {
			node = node->right;
		} else {
			return node;
		}

#else
		if (key < node->key) {
			node = node->left;
		} else if (key > node->key) {
			node = node->right;
		} else {
			return node;
		}	
#endif
	}
	return T->nil;
}


void rbtree_traversal(rbtree *T, rbtree_node *node) {
	if (node != T->nil) {
		rbtree_traversal(T, node->left);
#if ENABLE_KEY_CHAR
		printf("key:%s, value:%s\n", node->key, (char *)node->value);
#else
		printf("key:%d, color:%d\n", node->key, node->color);
#endif
		rbtree_traversal(T, node->right);
	}
}


#if 0

int main() {

#if ENABLE_KEY_CHAR

	char* keyArray[10] = {"King", "Darren", "Mark", "Vico", "Nick", "qiuxiang", "youzi", "taozi", "123", "234"};
	char* valueArray[10] = {"1King", "2Darren", "3Mark", "4Vico", "5Nick", "6qiuxiang", "7youzi", "8taozi", "9123", "10234"};

	rbtree *T = (rbtree *)malloc(sizeof(rbtree));
	if (T == NULL) {
		printf("malloc failed\n");
		return -1;
	}
	
	T->nil = (rbtree_node*)malloc(sizeof(rbtree_node));
	T->nil->color = BLACK;
	T->root = T->nil;

	rbtree_node *node = T->nil;
	int i = 0;
	for (i = 0;i < 10;i ++) {
		node = (rbtree_node*)malloc(sizeof(rbtree_node));
		
		node->key = malloc(strlen(keyArray[i]) + 1);
		memset(node->key, 0, strlen(keyArray[i]) + 1);
		strcpy(node->key, keyArray[i]);
		
		node->value = malloc(strlen(valueArray[i]) + 1);
		memset(node->value, 0, strlen(valueArray[i]) + 1);
		strcpy(node->value, valueArray[i]);

		rbtree_insert(T, node);
		
	}

	rbtree_traversal(T, T->root);
	printf("----------------------------------------\n");

	for (i = 0;i < 10;i ++) {

		rbtree_node *node = rbtree_search(T, keyArray[i]);
		rbtree_node *cur = rbtree_delete(T, node);
		free(cur);

		rbtree_traversal(T, T->root);
		printf("----------------------------------------\n");
	}

#else


	int keyArray[20] = {24,25,13,35,23, 26,67,47,38,98, 20,19,17,49,12, 21,9,18,14,15};

	rbtree *T = (rbtree *)malloc(sizeof(rbtree));
	if (T == NULL) {
		printf("malloc failed\n");
		return -1;
	}
	
	T->nil = (rbtree_node*)malloc(sizeof(rbtree_node));
	T->nil->color = BLACK;
	T->root = T->nil;

	rbtree_node *node = T->nil;
	int i = 0;
	for (i = 0;i < 20;i ++) {
		node = (rbtree_node*)malloc(sizeof(rbtree_node));
		node->key = keyArray[i];
		node->value = NULL;

		rbtree_insert(T, node);
		
	}

	rbtree_traversal(T, T->root);
	printf("----------------------------------------\n");

	for (i = 0;i < 20;i ++) {

		rbtree_node *node = rbtree_search(T, keyArray[i]);
		rbtree_node *cur = rbtree_delete(T, node);
		free(cur);

		rbtree_traversal(T, T->root);
		printf("----------------------------------------\n");
	}
#endif

	
}

#endif


//typedef struct _rbtree kvs_rbtree_t; 

//vs_rbtree_t global_rbtree;

// 5 + 2
// int kvs_rbtree_create(kvs_rbtree_t *inst) {

// 	if (inst == NULL) return 1;

// 	inst->nil = (rbtree_node*)kvs_malloc(sizeof(rbtree_node));
// 	inst->nil->color = BLACK;
// 	inst->root = inst->nil;

// 	return 0;

// }

kvs_rbtree_t* kvs_rbtree_create() {
	kvs_rbtree_t *inst = (kvs_rbtree_t *)kvs_malloc(sizeof(kvs_rbtree_t));
	if (inst == NULL) return NULL;

	inst->nil = (rbtree_node*)kvs_malloc(sizeof(rbtree_node));
	if (inst->nil == NULL) {
		kvs_free(inst, sizeof(kvs_rbtree_t));
		return NULL;
	}
	inst->nil->color = BLACK;
	inst->root = inst->nil;

	return inst;
}

void kvs_rbtree_destroy(kvs_rbtree_t *inst) {

	if (inst == NULL) return ;

	rbtree_node *node = NULL;

	while (!(node = inst->root)) {
		
		rbtree_node *mini = rbtree_mini(inst, node);
		
		rbtree_node *cur = rbtree_delete(inst, mini);
		kvs_free(cur, sizeof(rbtree_node));
		
	}

	kvs_free(inst->nil, sizeof(rbtree_node));

	return ;

}


int kvs_rbtree_set(kvs_rbtree_t *inst, char *key, char *value) {

	if (!inst || !key || !value) return -1;

	rbtree_node *node = (rbtree_node*)kvs_malloc(sizeof(rbtree_node));
		
	node->key = kvs_malloc(strlen(key) + 1);
	if (!node->key) return -2;
 	memset(node->key, 0, strlen(key) + 1);
	strcpy(node->key, key);
	
	node->value = kvs_malloc(strlen(value) + 1);
	if (!node->value) return -2;
	memset(node->value, 0, strlen(value) + 1);
	strcpy(node->value, value);

	rbtree_insert(inst, node);

	return 0;
}


char* kvs_rbtree_get(kvs_rbtree_t *inst, char *key)  {

	if (!inst || !key) return NULL;
	rbtree_node *node = rbtree_search(inst, key, strlen(key));
	if (!node) return NULL; // no exist
	if (node == inst->nil) return NULL;

	return node->value;
	
}

int kvs_rbtree_del(kvs_rbtree_t *inst, char *key) {

	if (!inst || !key) return -1;

	rbtree_node *node = rbtree_search(inst, key, strlen(key));
	if (!node) return 1; // no exist
	
	rbtree_node *cur = rbtree_delete(inst, node);
	free(cur);

	return 0;
}

int kvs_rbtree_mod(kvs_rbtree_t *inst, char *key, char *value) {

	if (!inst || !key || !value) return -1;

	rbtree_node *node = rbtree_search(inst, key, strlen(key));
	if (!node) return 1; // no exist
	if (node == inst->nil) return 1;
	
	kvs_free(node->value, strlen(node->value));;

	node->value = kvs_malloc(strlen(value));
	node->len_val = strlen(value);
	if (!node->value) return -2;
	
	memset(node->value, 0, node->len_val);
	memcpy(node->value, value, node->len_val);

	return 0;

}

int kvs_rbtree_exist(kvs_rbtree_t *inst, char *key) {

	if (!inst || !key) return -1;

	rbtree_node *node = rbtree_search(inst, key, strlen(key));
	if (!node) return 1; // no exist
	if (node == inst->nil) return 1;

	return 0;
}

/*
 *@return >=0 success -1 error -2 no exist
 */
int kvs_rbtree_resp_exist(kvs_rbtree_t *inst, char *key, int len_key) {

	if (!inst || !key) return -1;

	rbtree_node *node = rbtree_search(inst, key, len_key);
	if (!node) return -2; // no exist
	if (node == inst->nil) return -2; 

	return 0;
	
}

/*
 *@return >=0 success -1 error -2 exist
 */
int kvs_rbtree_resp_set(kvs_rbtree_t *inst, char *key, int len_key, char *value, int len_value) {

	if (!inst || !key || !value) return -1;

	int ret = kvs_rbtree_resp_exist(inst, key, len_key);
	if (ret == 0) return -2; // exist

	rbtree_node *node = (rbtree_node*)kvs_malloc(sizeof(rbtree_node));
		
	node->key = kvs_malloc(len_key);
	if (!node->key) return -1;
 	memset(node->key, 0, len_key);
	memcpy(node->key, key, len_key);
	node->len_key = len_key;
	
	node->value = kvs_malloc(len_value);
	if (!node->value) return -1;
	memset(node->value, 0, len_value);
	memcpy(node->value, value, len_value);
	node->len_val = len_value;

	rbtree_insert(inst, node);

	return 0;
}

int kvs_rbtree_resp_get(kvs_rbtree_t *inst, char *key, int len_key, char **value, int *len_value)  {

	if (!inst || !key || !value || !len_value) return -1;
	rbtree_node *node = rbtree_search(inst, key, len_key);
	if (!node) return -2; // no exist
	if (node == inst->nil) return -2;

	*value = node->value;
	*len_value = node->len_val;

	return 0;
	
}

int kvs_rbtree_resp_del(kvs_rbtree_t *inst, char *key, int len_key) {

	if (!inst || !key) return -1;

	rbtree_node *node = rbtree_search(inst, key, len_key);
	if (!node) return 1; // no exist
	
	rbtree_node *cur = rbtree_delete(inst, node);
	kvs_free(cur, sizeof(rbtree_node));

	return 0;
}

/*
 *@return >=0 success -1 error -2 no exist
 */
int kvs_rbtree_resp_mod(kvs_rbtree_t *inst, char *key, int len_key, char *value, int len_value) {

	if (!inst || !key || !value) return -1;

	rbtree_node *node = rbtree_search(inst, key, len_key);
	if (!node) return -2; // no exist
	if (node == inst->nil) return -2;
	
	kvs_free(node->value, node->len_val);;

	node->value = kvs_malloc(len_value);
	node->len_val = len_value;
	if (!node->value) return -1;
	
	memset(node->value, 0, node->len_val);
	memcpy(node->value, value, node->len_val);

	return 0;

}


int kvs_rbtree_filter(kvs_rbtree_t *inst, kvs_rbtree_item_filter filter, void* filter_ctx) {

	if (!inst || !filter) return -1;
	if (inst->root == inst->nil) return 0; // empty
	rbtree_node *node = rbtree_mini(inst, inst->root);
	while (node != inst->nil) {
		if(0 > filter(node->key, node->len_key, node->value, node->len_val, filter_ctx))
			return -1;
		node = rbtree_successor(inst, node);
	}

	return 0;
}