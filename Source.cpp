#include <dirent.h>
#include <string.h>
#include <vector>
#include <iostream>
#include <sstream>
#include <fstream>
#include <mpi.h>
#include <time.h>
#include <algorithm>
#include <map>
#define dim 4

using namespace std;

typedef vector<string> stringvec;

struct doc_count
{
	int nr;
	char* filename;
};

struct term_doc_count
{
	char* term;
	vector<doc_count> occ_in_file;
};

typedef map<char, vector<term_doc_count> > term_doc_count_map;

void read_directory(string &name, stringvec& v)
{
	DIR* dirp = opendir(name.c_str());
	struct dirent *dp;
	while ((dp = readdir(dirp)) != NULL)
	{
		v.push_back(dp->d_name);
	}
	closedir(dirp);
}

void setup_map(term_doc_count_map& map)
{
	for (int i = 97; i <= 122; i++)
	{
		map.insert(pair<char, vector<term_doc_count> >( (char)i, vector<term_doc_count>()));
	}
}

char** get_file_names(int files_nr, int arr_size, char* file_names)
{
	char** splitted_file_names = new char*[files_nr];
	int added_files = 0;
	int start = 0;
	for (int recv_chars = 0; recv_chars < arr_size; recv_chars++)
	{
		if (file_names[recv_chars] == ':')
		{
			if (start == 0)
			{
				splitted_file_names[added_files] = new char[recv_chars+1];
				memcpy(splitted_file_names[added_files], &file_names[start], recv_chars);
				splitted_file_names[added_files][recv_chars] = '\0';
				start = recv_chars;
				added_files++;
			}
			else
			{
				splitted_file_names[added_files] = new char[recv_chars - start];
				memcpy(splitted_file_names[added_files], &file_names[start + 1], recv_chars - start - 1);
				splitted_file_names[added_files][recv_chars - start - 1] = '\0';
				start = recv_chars;
				added_files++;
			}
		}
	}
	splitted_file_names[added_files] = new char[arr_size - start];
	memcpy(splitted_file_names[added_files], &file_names[start + 1],arr_size - start - 1);
	splitted_file_names[added_files][arr_size - start - 1] = '\0';
	return splitted_file_names;
}

char* write_to_file(term_doc_count_map term_mapping, int rank)
{
	ofstream write_to_aux_file;
	string aux_file_name = "auxiliar";
	char* rank_as_char = new char[2];
	rank_as_char[0] = '0' + rank;
	rank_as_char[1] = '\0';
	aux_file_name.append(rank_as_char);
	aux_file_name.append(".txt");

	term_doc_count_map::iterator map_itr;

	write_to_aux_file.open(aux_file_name);

		for (map_itr = term_mapping.begin(); map_itr != term_mapping.end(); map_itr++)
		{
			//cout << rank << "  " << "inside map" << "  " << map_itr->second.size() << endl;
			for (int i = 0; i < map_itr->second.size(); i++)
			{
				//cout << rank << "   " << "inside vector of terms" << endl;
				write_to_aux_file << map_itr->second[i].term << "  ";
				for (int j = 0; j < map_itr->second[i].occ_in_file.size(); j++)
				{
					//cout << rank << "   " << "inside vector of particular term" << endl;
					write_to_aux_file << map_itr->second[i].occ_in_file[j].filename << "  " << map_itr->second[i].occ_in_file[j].nr << "  ";
				}
				write_to_aux_file << endl;
			}
		}

	char* file_name_to_send = new char[aux_file_name.length() + 1];
	strcpy_s(file_name_to_send, aux_file_name.length() + 1, aux_file_name.c_str());
	return file_name_to_send;
}

void write_to_file_master(char** filenames, int vector_size, string final_dir)
{
	ofstream write_to_file;
	ifstream files_from_workers;

	map <char, stringvec> filenames_map;

	for (int i = 97; i <= 122; i++)
	{
		filenames_map.insert(pair<char, stringvec>((char)i, stringvec()));
	}
	for (int i = 1; i < vector_size; i++)
	{
		cout << filenames[i] << endl;
		files_from_workers.open(filenames[i]);
		//string full = final_dir;
		string line;

		map<char, stringvec> ::iterator filenames_map_it;
		while (getline(files_from_workers, line))
		{
			//cout << line;
			int space_index = line.find_first_of(" ");
			string word = line.substr(0, space_index);
			string app_in_files = line.substr(space_index + 1, line.length() - space_index - 1);

			char first_letter = word.c_str()[0];
			
			for (filenames_map_it = filenames_map.begin(); filenames_map_it != filenames_map.end(); filenames_map_it++)
			{
				if (filenames_map_it->first == first_letter)
				{
					int vector_dim = filenames_map_it->second.size();
					bool term_found = false;
					for (int j = 0; j < vector_dim; j++)
					{
						if (filenames_map_it->second[j] == word)
						{
							string full_path = final_dir;
							full_path.append("\\");
							full_path.append(word);
							full_path.append(".txt");
							write_to_file.open(full_path, ios::app);
							write_to_file << "   ";
							write_to_file << app_in_files;
							write_to_file.close();
							term_found = true;
							break;
						}
					}

					if (!term_found)
					{
						string full_path = final_dir;
						full_path.append("\\");
						full_path.append(word);
						full_path.append(".txt");
						write_to_file.open(full_path);
						write_to_file << "   ";
						write_to_file << app_in_files;
						write_to_file.close();

						filenames_map_it->second.push_back(word);
						break;
					}
				}
			}

		}
		files_from_workers.close();
	}
}

int main(int argc, char** argv)
{


	int size;
	int rank;
	stringvec dir_content;
	MPI_Init(&argc, &argv);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);


	if (rank == 0)
	{
		string dir = "E:\\Downloads\\iles";
		read_directory(dir, dir_content);
		int files_per_process = (dir_content.size() - 2) / (size-1);
		int remaining_files = (dir_content.size() - 2) % (size-1);
		int reserved_files = 2;
		
		for (int process_index = 1; process_index < size; process_index++)
		{
			int files_for_actual_proc;
			if (remaining_files != 0)
			{
				files_for_actual_proc = files_per_process + 1;
				remaining_files--;
			}
			else
			{
				files_for_actual_proc = files_per_process;
			}
			int limit = files_for_actual_proc + reserved_files;
			string files_to_send = "";
			for (int file_idx = reserved_files; file_idx < limit; file_idx++)
			{
				files_to_send.append(dir_content.at(file_idx));
				if (file_idx < limit - 1)
				{
					files_to_send.append(":");
				}
			}
			reserved_files = limit;
			int file_total_len = files_to_send.length();

			MPI_Send(&files_for_actual_proc, 1, MPI_INT, process_index, 1, MPI_COMM_WORLD);
			MPI_Send(&file_total_len, 1, MPI_INT, process_index, 1, MPI_COMM_WORLD);
			MPI_Send((void*)files_to_send.c_str(), file_total_len, MPI_CHAR, process_index, 1, MPI_COMM_WORLD);
		}

		char** auxiliary_files = new char*[size];
		int* filename_length = new int[size];
		
		for (int i = 1; i < size; i++)
		{
			cout << "Waiting for process send" << endl;
			MPI_Recv(&filename_length[i], 1, MPI_INT, i , 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			cout << filename_length[i] << endl;
			auxiliary_files[i] = new char[filename_length[i] + 1];
			MPI_Recv(auxiliary_files[i], filename_length[i], MPI_CHAR, i , 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			auxiliary_files[i][filename_length[i]] = '\0';
			cout << auxiliary_files[i] << endl;
		}
		string full_path = "E:\\AC-CTI\\Anul4Sem1\\ALPD\\TemaDeCasa";
		cout << "Function call" << endl;
		write_to_file_master(auxiliary_files, size, full_path);
	}
	
	else
	{
		clock_t start = clock();
		int files_nr;
		int incoming_arr_size;
		char* file_names;

		MPI_Recv(&files_nr, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		MPI_Recv(&incoming_arr_size, 1, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		file_names = new char[incoming_arr_size];		
		MPI_Recv(file_names, incoming_arr_size, MPI_CHAR, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		char** splitted_file_names = get_file_names(files_nr, incoming_arr_size, file_names);

		term_doc_count_map term_mapping;
		setup_map(term_mapping);
		term_doc_count_map::iterator map_itr;

		string dir = "E:\\Downloads\\iles";

		for (int processed_files = 0; processed_files < files_nr; processed_files++)
		{
			string file = dir + "\\" + splitted_file_names[processed_files];
			ifstream read_file(file);

			string aux;
			int pos;
			clock_t tStart = clock();

					while (read_file >> aux)
					{
						bool term_found = false;
						bool same_file = false;
						pos = aux.find_first_of(",.!?", 0);
						if (pos != string::npos)
							aux = aux.substr(0, pos);
			
						if (aux.length() > 2)
						{
							transform(aux.begin(), aux.end(), aux.begin(), tolower);
			
							char first_letter = aux.c_str()[0];
							int term_location;
							for (map_itr = term_mapping.begin(); map_itr != term_mapping.end(); map_itr++)
							{
								if (first_letter == map_itr->first)
								{
									int vector_dim = map_itr->second.size();
									for (int i = 0; i < vector_dim; i++)
									{
										if (map_itr->second[i].term == aux)
										{
											term_found = true;
											term_location = i;
											int list_doc_count = map_itr->second[i].occ_in_file.size();
											for (int j = 0; j < list_doc_count; j++)
											{
												if ( strcmp(map_itr->second[i].occ_in_file[j].filename, splitted_file_names[processed_files]) == 0)
												{
													same_file = true;
													map_itr->second[i].occ_in_file[j].nr++;
													break;
												}
											}
											break;
										}
									}
									break;
								}
							}
			
							if (same_file == false && term_found == true)
							{
								for (map_itr = term_mapping.begin(); map_itr != term_mapping.end(); map_itr++)
								{
									if (first_letter == map_itr->first)
									{
										doc_count new_occ = doc_count();
										new_occ.filename = new char[strlen ( splitted_file_names[processed_files] ) + 1];
										strcpy_s(new_occ.filename, strlen(splitted_file_names[processed_files]) + 1, splitted_file_names[processed_files] );
										//*new_occ.filename = *file_name.c_str();
										new_occ.nr = 1;
										map_itr->second[term_location].occ_in_file.push_back(new_occ);
										//cout << new_occ.filename << "  " << new_occ.nr << endl;
									}
								}
							}
							else if (term_found == false)
							{
								for (map_itr = term_mapping.begin(); map_itr != term_mapping.end(); map_itr++)
								{
									if (first_letter == map_itr->first)
									{
										doc_count new_occ = doc_count();
										new_occ.nr = 1;
										new_occ.filename = new char[strlen(splitted_file_names[processed_files]) + 1];
										strcpy_s(new_occ.filename, strlen(splitted_file_names[processed_files]) + 1, splitted_file_names[processed_files]);			
			
										term_doc_count new_term = term_doc_count();
										new_term.term = new char[aux.length() + 1];
										strcpy_s(new_term.term, aux.length() + 1, aux.c_str());
										new_term.occ_in_file.push_back(new_occ);
										map_itr->second.push_back(new_term);
									}
								}
							}
						}
					}
			
		}

		char* filename_to_send = write_to_file(term_mapping, rank);
		int buffer = strlen(filename_to_send);
		
		//MPI_Barrier(MPI_COMM_WORLD);
		MPI_Send(&buffer, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
		MPI_Send(filename_to_send, strlen(filename_to_send), MPI_CHAR, 0, 1, MPI_COMM_WORLD);
		
		printf("RANK = %d Time taken: %.2fs\n", rank, (double)(clock() - start) / CLOCKS_PER_SEC);

		MPI_Barrier(MPI_COMM_WORLD);
	}

	MPI_Finalize();

	return 0;
}
